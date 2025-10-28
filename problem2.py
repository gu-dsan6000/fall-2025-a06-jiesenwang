import argparse
import os
import sys
import shutil
from urllib.parse import urlparse

from pyspark.sql import SparkSession, functions as F

# ---------- Helpers: FS detection + single-file writers (same idea as problem1) ----------
def is_remote_fs(path: str) -> bool:
    scheme = urlparse(path).scheme
    return scheme in ("hdfs", "s3", "s3a", "s3n", "gs", "abfs")

def hdfs_rename_single_part(spark, tmp_dir: str, final_path: str, ext: str):
    if not is_remote_fs(tmp_dir):
        import glob
        part = None
        for p in glob.glob(os.path.join(tmp_dir, f"part-*{ext}")):
            part = p
            break
        if part is None:
            raise RuntimeError(f"No part file with extension {ext} found in {tmp_dir}")
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        shutil.move(part, final_path)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    URI = jvm.java.net.URI
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(URI(tmp_dir), conf)
    tmpPath = Path(tmp_dir)
    statuses = fs.listStatus(tmpPath)
    part_path = None
    for st in statuses:
        name = st.getPath().getName()
        if name.startswith("part-") and name.endswith(ext):
            part_path = st.getPath()
            break
    if part_path is None:
        raise RuntimeError(f"No part file with extension {ext} found in {tmp_dir}")
    finalPath = Path(final_path)
    parent = finalPath.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)
    ok = fs.rename(part_path, finalPath)
    if not ok:
        raise RuntimeError(f"Failed to rename {part_path} to {finalPath}")
    fs.delete(tmpPath, True)

def write_single_csv(df, spark, final_csv_path: str):
    tmp_dir = final_csv_path + "_tmp"
    (df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir))
    hdfs_rename_single_part(spark, tmp_dir, final_csv_path, ".csv")

# ---------- Parsing & Aggregation ----------
def make_spark(app_name: str, master: str | None):
    b = SparkSession.builder.appName(app_name)
    if master:
        b = b.master(master)
    return b.getOrCreate()

def extract_timestamps(df):
    """
    Extract timestamp from common Spark/YARN log prefixes.
    Supports:
      - 'YY/MM/DD HH:MM:SS ...'  e.g., '17/03/29 10:04:41 ...'
      - 'YYYY-MM-DD[ T]HH:MM:SS ...'
    Returns df with a 'ts' (TimestampType) column.
    """
    # Pattern A: 17/03/29 10:04:41
    yy = F.regexp_extract("value", r"(\d{2})/(\d{2})/(\d{2}) (\d{2}:\d{2}:\d{2})", 1)
    mm = F.regexp_extract("value", r"(\d{2})/(\d{2})/(\d{2}) (\d{2}:\d{2}:\d{2})", 2)
    dd = F.regexp_extract("value", r"(\d{2})/(\d{2})/(\d{2}) (\d{2}:\d{2}:\d{2})", 3)
    hhmmss = F.regexp_extract("value", r"(\d{2})/(\d{2})/(\d{2}) (\d{2}:\d{2}:\d{2})", 4)
    ts_a = F.when(yy != "", F.concat(F.lit("20"), yy, F.lit("-"), mm, F.lit("-"), dd, F.lit(" "), hhmmss))

    # Pattern B: 2017-03-29 10:04:41  或 2017-03-29T10:04:41
    ts_b = F.regexp_extract("value", r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", 1)
    ts_b2 = F.regexp_extract("value", r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", 2)
    ts_b_full = F.when(ts_b != "", F.concat(ts_b, F.lit(" "), ts_b2))

    ts_str = F.coalesce(ts_b_full, ts_a)
    return df.withColumn("ts", F.to_timestamp(ts_str, "yyyy-MM-dd HH:mm:ss"))

def build_timeline(spark, input_path_glob: str):
    # Read all text files (recursively if needed)
    input_glob = input_path_glob
    raw = spark.read.text(input_glob).withColumn("path", F.input_file_name())
    raw = extract_timestamps(raw).filter(F.col("ts").isNotNull())

    # From path, derive application_id & cluster_id & app_number
    # path example .../application_1485248649253_0001/...
    app_id = F.regexp_extract("path", r"(application_\d+_\d+)", 1)
    cluster_id = F.regexp_extract(app_id, r"application_(\d+)_\d+", 1)
    app_seq = F.regexp_extract(app_id, r"application_\d+_(\d+)", 1)
    app_number = F.lpad(app_seq, 4, "0")

    timeline = (raw
        .withColumn("application_id", app_id)
        .withColumn("cluster_id", cluster_id)
        .withColumn("app_number", app_number)
        .groupBy("cluster_id", "application_id", "app_number")
        .agg(F.min("ts").alias("start_time"),
             F.max("ts").alias("end_time"))
    )

    # Format as strings for CSV
    timeline_fmt = (timeline
        .select(
            "cluster_id",
            "application_id",
            "app_number",
            F.date_format("start_time", "yyyy-MM-dd HH:mm:ss").alias("start_time"),
            F.date_format("end_time", "yyyy-MM-dd HH:mm:ss").alias("end_time"),
        )
        .orderBy("cluster_id", "app_number")
    )
    return timeline_fmt

def build_cluster_summary(timeline_df):
    # Convert back to timestamp to compute min/max by cluster
    tl = (timeline_df
          .withColumn("start_ts", F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("end_ts", F.to_timestamp("end_time", "yyyy-MM-dd HH:mm:ss")))

    per_cluster = (tl.groupBy("cluster_id")
        .agg(
            F.countDistinct("application_id").alias("num_applications"),
            F.date_format(F.min("start_ts"), "yyyy-MM-dd HH:mm:ss").alias("cluster_first_app"),
            F.date_format(F.max("end_ts"), "yyyy-MM-dd HH:mm:ss").alias("cluster_last_app"),
        )
        .orderBy(F.col("num_applications").desc())
    )
    return per_cluster

def write_stats_txt(spark, summary_df, timeline_df, out_txt):
    total_clusters = summary_df.count()
    total_apps = timeline_df.select("application_id").distinct().count()
    avg_apps = (total_apps / total_clusters) if total_clusters > 0 else 0.0

    top = (summary_df
           .orderBy(F.col("num_applications").desc())
           .limit(5)
           .collect())

    lines = []
    lines.append(f"Total unique clusters: {total_clusters}")
    lines.append(f"Total applications: {total_apps}")
    lines.append(f"Average applications per cluster: {avg_apps:.2f}")
    lines.append("")
    lines.append("Most heavily used clusters:")
    for r in top:
        lines.append(f"  Cluster {r['cluster_id']}: {r['num_applications']} applications")

    tmp_dir = out_txt + "_tmp"
    (spark.createDataFrame([(l,) for l in lines], ["value"])
         .coalesce(1)
         .write.mode("overwrite")
         .text(tmp_dir))
    hdfs_rename_single_part(spark, tmp_dir, out_txt, "")

def make_plots_from_csvs(out_dir):
    import pandas as pd
    import matplotlib.pyplot as plt
    try:
        import seaborn as sns
    except Exception:
        sns = None

    tl = pd.read_csv(os.path.join(out_dir, "problem2_timeline.csv"))
    cs = pd.read_csv(os.path.join(out_dir, "problem2_cluster_summary.csv"))

    # ---- Bar chart: #apps per cluster ----
    plt.figure(figsize=(10, 6))
    if sns:
        ax = sns.barplot(data=cs, x="cluster_id", y="num_applications")
    else:
        ax = plt.bar(cs["cluster_id"].astype(str), cs["num_applications"])
    if sns:
        for i, v in enumerate(cs["num_applications"].tolist()):
            plt.text(i, v, str(v), ha="center", va="bottom", fontsize=10)
    else:
        for i, v in enumerate(cs["num_applications"].tolist()):
            plt.text(i, v, str(v), ha="center", va="bottom", fontsize=10)
    plt.xlabel("Cluster ID")
    plt.ylabel("Applications")
    plt.title("Applications per Cluster")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "problem2_bar_chart.png"))
    plt.close()

    # ---- Density plot: durations (largest cluster) ----
    # pick cluster with max apps
    if len(cs) > 0:
        largest_cluster = cs.sort_values("num_applications", ascending=False)["cluster_id"].iloc[0]
        tl2 = tl[tl["cluster_id"] == largest_cluster].copy()
        # compute duration (minutes)
        tl2["start_time"] = pd.to_datetime(tl2["start_time"])
        tl2["end_time"] = pd.to_datetime(tl2["end_time"])
        tl2["duration_min"] = (tl2["end_time"] - tl2["start_time"]).dt.total_seconds() / 60.0
        plt.figure(figsize=(10, 6))
        if sns:
            sns.histplot(tl2["duration_min"], bins=40, kde=True)
        else:
            plt.hist(tl2["duration_min"], bins=40)
        plt.xscale("log")
        plt.xlabel("Duration (minutes, log scale)")
        plt.ylabel("Count")
        plt.title(f"Duration Distribution (Cluster {largest_cluster}, n={len(tl2)})")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "problem2_density_plot.png"))
        plt.close()

def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("spark_master", nargs="?", default=None,
                        help="Spark master URL (e.g., spark://<MASTER_PRIVATE_IP>:7077). Optional if --skip-spark.")
    parser.add_argument("--net-id", default="unknown", help="Your net id (for logging only).")
    parser.add_argument("--input", default="data/raw/*/*",
                        help="Input glob for logs (default: data/raw/*/*).")
    parser.add_argument("--output", default="data/output/",
                        help="Output directory (default: data/output/).")
    parser.add_argument("--skip-spark", action="store_true",
                        help="Skip Spark processing and only (re)generate visuals/stats from existing CSVs.")
    args = parser.parse_args()

    out_dir = args.output
    os.makedirs(out_dir, exist_ok=True)
    out_timeline = os.path.join(out_dir, "problem2_timeline.csv")
    out_summary = os.path.join(out_dir, "problem2_cluster_summary.csv")
    out_stats = os.path.join(out_dir, "problem2_stats.txt")

    if args.skip_spark:
        # Just re-make charts & stats from existing CSVs
        # Build basic stats text if missing
        try:
            make_plots_from_csvs(out_dir)
        except Exception as e:
            print(f"[skip-spark] Failed to make plots from CSVs in {out_dir}: {e}", file=sys.stderr)
            sys.exit(2)
        print(f"[skip-spark] Wrote plots to {out_dir}")
        sys.exit(0)

    spark = make_spark("Problem2_ClusterUsageAnalysis", args.spark_master)

    # 1) timeline
    timeline_df = build_timeline(spark, args.input)
    write_single_csv(timeline_df, spark, out_timeline)

    # 2) summary
    summary_df = build_cluster_summary(timeline_df)
    write_single_csv(summary_df, spark, out_summary)

    # 3) stats.txt
    write_stats_txt(spark, summary_df, timeline_df, out_stats)

    # 4) visualizations from CSVs
    spark.catalog.clearCache()  # free mem before pandas read
    make_plots_from_csvs(out_dir)

    print("Wrote:")
    print(" ", out_timeline)
    print(" ", out_summary)
    print(" ", out_stats)
    print(" ", os.path.join(out_dir, "problem2_bar_chart.png"))
    print(" ", os.path.join(out_dir, "problem2_density_plot.png"))
    spark.stop()

if __name__ == "__main__":
    sys.exit(main())
