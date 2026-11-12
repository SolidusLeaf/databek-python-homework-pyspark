# Clickstream Sessionization with PySpark Window Functions

Given clickstream events `(user_id, ts, url)`, this job assigns a session id to
every event (a new session starts after 30 minutes of inactivity) using PySpark
window functions, then computes **sessions per day** and **average session
length**.

## Session rules (assumptions)

The task leaves a few details open, so these are the rules the code follows:

| Topic | Rule |
|---|---|
| Timeout | A new session starts when the gap to the user's previous event is **strictly greater than 30 minutes** (> 1800 s). A gap of exactly 30:00 stays in the same session. |
| Session day | A session counts toward the calendar day (UTC) on which it **started**, so a session crossing midnight is counted once. |
| Session length | `last event ts - first event ts`, in seconds. A one-event session has length 0. |
| Ties | Events with identical timestamps are ordered by `url` so results are deterministic. |

## How it works

All the window logic lives in `assign_sessions()`:

1. `Window.partitionBy("user_id").orderBy("ts", "url")`
2. `lag("ts")` gives the user's previous event time; the gap is computed in
   seconds with `unix_timestamp`.
3. `is_new_session = 1` if there is no previous event or the gap is > 1800 s,
   otherwise 0 (the null case is handled explicitly).
4. A running `sum(is_new_session)` over the same window (frame: unbounded
   preceding to current row) gives a per-user session counter.
5. `session_id = user_id + "_" + counter`.

Then `summarize_sessions()` collapses to one row per session (start, end,
length, event count) and `daily_metrics()` groups by start date.

## Repository layout

```
.
├── README.md
├── requirements.txt
├── sessionize.py            # the PySpark job
├── data/
│   ├── events.csv           # synthetic sample input (11,020 events)
│   └── ground_truth.json    # expected results, computed without Spark
└── out/
    └── daily_metrics.csv    # result of the job on the sample data
```

## Sample data

`data/events.csv` is **synthetic**: 200 users over 7 days (2026-11-01 to
2026-11-07), with columns `user_id, ts, url`. It is shuffled, and includes
deliberate edge cases: a session that crosses midnight, identical timestamps,
a single-event user, and gaps of exactly 30 minutes.

`data/ground_truth.json` holds the expected sessions per day and average
length, computed with plain Python (no Spark), so the Spark result can be
checked independently.

## Requirements

- Python 3.9+ (developed with 3.12)
- **Java 17 or 21** (PySpark 4.x does not run on Java 8)
- PySpark (see `requirements.txt`)

```
python -m pip install -r requirements.txt
```

On Windows use `py -m pip install -r requirements.txt`.

## Run

```
python sessionize.py --input data/events.csv --output out/daily_metrics.csv --truth data/ground_truth.json
```

Arguments:

| Argument | Default | Meaning |
|---|---|---|
| `--input` | `data/events.csv` | Input CSV with `user_id,ts,url` (`ts` as `yyyy-MM-dd HH:mm:ss`) |
| `--output` | `out/daily_metrics.csv` | Output file (a single CSV file, not a folder) |
| `--truth` | none | Optional `ground_truth.json`; prints `VALIDATION PASSED` or the mismatches |

To keep a log of a run, redirect both output streams:

```
python sessionize.py ... > run.log 2>&1          # cmd / bash
python sessionize.py ... *> run.log              # PowerShell
```

## Expected result

| session_date | sessions | avg_session_length_s |
|---|---|---|
| 2026-11-01 | 232 | 1722.32 |
| 2026-11-02 | 246 | 1705.47 |
| 2026-11-03 | 237 | 1771.82 |
| 2026-11-04 | 239 | 1613.02 |
| 2026-11-05 | 224 | 1702.42 |
| 2026-11-06 | 240 | 1742.79 |
| 2026-11-07 | 228 | 1625.73 |
| 2026-11-08 | 14 | 1597.5 |

1,660 sessions in total. The 14 sessions on 2026-11-08 are late visits by users
whose activity spilled past the last simulated day; this is expected.

## Notes and warnings

- **Output is written with Python's `csv` module, not Spark's writer.** On
  Windows, `DataFrame.write.csv()` needs Hadoop's `winutils.exe` and fails with
  `HADOOP_HOME and hadoop.home.dir are unset` if it is missing. The daily
  result is only a few rows, so it is collected to the driver and written with
  plain Python. For large outputs, use Spark's writer (Linux/macOS, or Windows
  with Hadoop configured).
- **Harmless warnings:** `Did not find winutils.exe`, `NativeCodeLoader`, and
  the pandas >= 3.0 `FutureWarning` can be ignored; pandas is not used by this
  job.
- **Java version:** if the job fails at startup, run `java -version`. Java 8
  is too old for PySpark 4.x. After installing a newer JDK, make sure
  `JAVA_HOME` points to it, `%JAVA_HOME%\bin` comes first in `PATH`, and
  restart your terminal or IDE (an IDE may keep the old environment until
  restarted). Run `where java` (Windows) or `which -a java` to see which one
  wins.
- **Windows and `py`:** if Spark cannot find Python workers, set
  `PYSPARK_PYTHON` to the full path of your `python.exe`.
- **Time zone:** the Spark session time zone is pinned to UTC so session days
  do not depend on the machine's local time zone.

## Limitations

- Rows with a null `user_id` or `ts` are not cleaned or filtered; they should
  be dropped or handled upstream.
- All events of one user are processed in a single partition. This is fine for
  normal users but would be slow for extreme outliers (for example bots with
  millions of events).
- Timestamps are treated as UTC with second precision.
- Run on a single machine (`local[*]`); no cluster configuration is included.

## Privacy

The sample data is synthetic. Real clickstream data (user ids, URLs,
timestamps) is personal data: do not commit it to a public repository.