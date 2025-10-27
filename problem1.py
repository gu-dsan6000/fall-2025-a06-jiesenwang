import argparse
import os
import shutil
import sys
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]

def make_spark(app_name="Problem1_LogLevelDistribution"):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

def is_remote_fs(path: str) -> bool:
    scheme = urlparse(path).scheme
    return scheme in ("hdfs", "s3", "s3a", "s3n", "gs", "abfs")

def hdfs_rename_single_part(spark, tmp_dir: str, final_path: str, ext: str):
    """
    Rename part-* file inside tmp_dir to final_path (single file), then delete tmp_dir.
    Works for HDFS/S3A/etc via Hadoop FS; falls back to local rename if scheme is empty.
    """
    if not is_remote_fs(tmp_dir):
        # Local filesystem
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
    # Ensure parent dir exists
    parent = finalPath.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)
    ok = fs.rename(part_path, finalPath)
    if not ok:
        raise RuntimeError(f"Failed to rename {part_path} to {finalPath}")
    fs.delete(tmpPath, True)

def write_single_csv(df, spark, final_csv_path: str):
    tmp_dir = final_csv_path + "_tmp"
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", "true")
       .csv(tmp_dir))
    hdfs_rename_single_part(spark, tmp_dir, final_csv_path, ".csv")

def write_single_txt(lines, spark, final_txt_path: str):
    # lines: list[str]; write as text (one line per row)
    tmp_dir = final_txt_path + "_tmp"
    line_df = spark.createDataFrame([(l,) for l in lines], ["value"])
    (line_df.coalesce(1)
            .write.mode("overwrite")
            .text(tmp_dir))
    hdfs_rename_single_part(spark, tmp_dir, final_txt_path, "")

def main():
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    parser.add_argument("--input", default="data/sample/",
                        help="Input directory (can be local, hdfs://, s3a://). Defaults to data/sample/")
    parser.add_argument("--output", default="data/output/",
                        help="Output directory (local or remote). Defaults to data/output/")
    parser.add_argument("--sample-size", type=int, default=10, help="Random sample size (default: 10)")
    args = parser.parse_args()

    spark = make_spark()

    # Read all text files (recursive via glob)
    input_glob = args.input if args.input.endswith("*") else os.path.join(args.input, "**/*")
    df = (spark.read.text(input_glob))
    total_lines = df.count()

    # Extract log level using regex anywhere in the line
    # Pattern: word boundary then one of the levels then boundary
    pattern = r".*\b(INFO|WARN|ERROR|DEBUG)\b.*"
    df_levels = (
        df.withColumn("log_level", F.regexp_extract(F.col("value"), pattern, 1))
          .withColumn("log_level", F.when(F.col("log_level") == "", None).otherwise(F.col("log_level")))
          .withColumnRenamed("value", "log_entry")
    )
    lines_with_levels = df_levels.filter(F.col("log_level").isNotNull())
    total_with_levels = lines_with_levels.count()
    unique_levels = lines_with_levels.select("log_level").distinct().count()

    # Counts
    counts_df = (
        lines_with_levels.groupBy("log_level").count()
                         .orderBy(F.col("count").desc())
    )
    # Ensure only expected levels appear and keep deterministic order when counts tie
    counts_df = counts_df

    # Random sample (stable randomness per run seed)
    sample_df = (
        lines_with_levels.orderBy(F.rand(seed=42))
                         .select("log_entry", "log_level")
                         .limit(args.sample_size)
    )

    # Write CSV outputs (single files)
    out_counts = os.path.join(args.output, "problem1_counts.csv")
    out_sample = os.path.join(args.output, "problem1_sample.csv")
    write_single_csv(counts_df, spark, out_counts)
    write_single_csv(sample_df, spark, out_sample)

    # Build summary text
    # Collect counts to driver for pretty percentages
    counts = {row["log_level"]: row["count"] for row in counts_df.collect()}
    def pct(n):
        return (n / total_with_levels * 100.0) if total_with_levels > 0 else 0.0

    lines = []
    lines.append(f"Total log lines processed: {total_lines:,}")
    lines.append(f"Total lines with log levels: {total_with_levels:,}")
    lines.append(f"Unique log levels found: {unique_levels}")
    lines.append("")
    lines.append("Log level distribution:")
    # Print in a consistent order
    for lvl in sorted(counts.keys()):
        c = counts[lvl]
        lines.append(f"  {lvl:<5}: {c:>10,} ({pct(c):6.2f}%)")

    out_summary = os.path.join(args.output, "problem1_summary.txt")
    write_single_txt(lines, spark, out_summary)

    print(f"Wrote:\n  {out_counts}\n  {out_sample}\n  {out_summary}")

    spark.stop()

if __name__ == "__main__":
    sys.exit(main())

