"""Sessionize clickstream events with PySpark window functions.

Rules:
  * new session when the gap to the user's previous event is > 30 min (strict)
  * a session belongs to the day it STARTED
  * session length = last event ts - first event ts (seconds)

Run:
    python sessionize.py --input data/events.csv --output out/daily_metrics.csv \
        --truth data/ground_truth.json
"""
import argparse
import csv
import json
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

TIMEOUT_S = 30 * 60


# ---------------------------------------------------------------- step 1
def read_events(spark: SparkSession, path: str) -> DataFrame:
    """Read the CSV with an explicit schema (pandas: dtype= / parse_dates=)."""
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("url", StringType(), True),
    ])
    return (spark.read
            .option("header", True)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(schema)
            .csv(path))


# ------------------------------------------------------------- steps 2-5
def assign_sessions(df: DataFrame, timeout_s: int = TIMEOUT_S) -> DataFrame:
    """Add session_id using window functions. This is the core of the task."""
    # One window per user, rows ordered by time. "url" is a tiebreaker so rows
    # with identical timestamps are ordered deterministically.
    # pandas: df.sort_values(["user_id","ts"]).groupby("user_id")
    w = Window.partitionBy("user_id").orderBy("ts", "url")

    # Same window, but with an explicit "from first row up to this row" frame,
    # needed for a running total.  pandas: .groupby("user_id").cumsum()
    w_running = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return (
        df
        # previous event's timestamp within the same user (null for the first)
        # pandas: .groupby("user_id")["ts"].shift(1)
        .withColumn("prev_ts", F.lag("ts").over(w))
        # gap in seconds; null when prev_ts is null
        .withColumn("gap_s", F.unix_timestamp("ts") - F.unix_timestamp("prev_ts"))
        # 1 = this event starts a new session. The isNull() check is explicit
        # because "null > 1800" is null (neither True nor False) in Spark.
        .withColumn(
            "is_new_session",
            F.when(F.col("prev_ts").isNull() | (F.col("gap_s") > timeout_s), 1)
             .otherwise(0),
        )
        # running sum of the flags = session counter per user (1, 2, 3, ...)
        .withColumn("session_num", F.sum("is_new_session").over(w_running))
        # globally unique id
        .withColumn("session_id", F.concat_ws("_", "user_id", "session_num"))
    )


# ---------------------------------------------------------------- step 6
def summarize_sessions(events: DataFrame) -> DataFrame:
    """One row per session: start, end, length, number of events."""
    return (
        events
        .groupBy("user_id", "session_id")
        .agg(
            F.min("ts").alias("session_start"),
            F.max("ts").alias("session_end"),
            F.count("*").alias("n_events"),
        )
        .withColumn(
            "session_length_s",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
        )
    )


# ---------------------------------------------------------------- step 7
def daily_metrics(sessions: DataFrame) -> DataFrame:
    """Sessions per day and average session length (seconds)."""
    return (
        sessions
        .withColumn("session_date", F.to_date("session_start"))
        .groupBy("session_date")
        .agg(
            F.count("*").alias("sessions"),
            F.round(F.avg("session_length_s"), 2).alias("avg_session_length_s"),
        )
        .orderBy("session_date")
    )


# ------------------------------------------------------------- output
def write_csv(df: DataFrame, path: str) -> None:
    """Write a SMALL result as one CSV file with plain Python.

    df.write.csv() goes through Hadoop's file layer, which needs winutils.exe
    on Windows. The daily result is only a few rows, so collecting it to the
    driver is safe and avoids that dependency. (For large outputs, use Spark's
    writer on Linux/macOS or a configured Windows Hadoop setup instead.)
    """
    rows = df.collect()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        for r in rows:
            writer.writerow(list(r))


# ---------------------------------------------------------------- step 8
def validate(daily: DataFrame, truth_path: str) -> bool:
    """Compare Spark output with the plain-Python ground truth."""
    with open(truth_path) as f:
        truth = json.load(f)["per_day"]
    got = {
        str(r["session_date"]): (r["sessions"], r["avg_session_length_s"])
        for r in daily.collect()
    }
    ok = True
    for day, exp in truth.items():
        g = got.get(day)
        if g is None or g[0] != exp["sessions"] or abs(g[1] - exp["avg_length_s"]) > 0.01:
            print(f"MISMATCH {day}: spark={g} expected=({exp['sessions']}, {exp['avg_length_s']})")
            ok = False
    if set(got) != set(truth):
        print("MISMATCH: different set of days", set(got) ^ set(truth))
        ok = False
    print("VALIDATION PASSED" if ok else "VALIDATION FAILED")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/events.csv")
    ap.add_argument("--output", default="out/daily_metrics.csv")
    ap.add_argument("--truth", default=None, help="optional ground_truth.json")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("sessionize")
             .master("local[*]")
             # to_date() uses the session time zone; pin it so days match
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    events = read_events(spark, args.input)
    with_sessions = assign_sessions(events)
    sessions = summarize_sessions(with_sessions)
    daily = daily_metrics(sessions)

    print("--- sample events with session ids")
    with_sessions.orderBy("user_id", "ts").show(15, truncate=False)
    print("--- daily metrics")
    daily.show(truncate=False)

    if args.truth:
        validate(daily, args.truth)

    write_csv(daily, args.output)
    print(f"Wrote {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()