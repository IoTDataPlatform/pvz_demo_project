import math
import os
import subprocess
import time
from pathlib import Path

import pandas as pd
import psycopg

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "latency_benchmark" / "benchmark-output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PG_HOST = "127.0.0.1"
PG_PORT = 5430
PG_DB = "db"
PG_USER = "app"
PG_PASSWORD = "app"

DEVICE_COUNTS = [10, 20, 30, 40, 50, 60, 70]
REALTIME_INTERVAL_SEC = 1
USELESS_PAYLOAD_BYTES = 0
RUN_SECONDS = 300

RESULTS_CSV = OUTPUT_DIR / "count_benchmark_metrics.csv"


def p(series: pd.Series, q: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return math.nan
    return float(s.quantile(q))


def mean(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return math.nan
    return float(s.mean())


def start_platform(device_count: int, prefix: str):
    env = os.environ.copy()
    env["DEVICE_COUNT"] = str(device_count)
    env["DEVICE_PREFIX"] = prefix
    env["REALTIME_INTERVAL_SEC"] = str(REALTIME_INTERVAL_SEC)
    env["USELESS_PAYLOAD_BYTES"] = str(USELESS_PAYLOAD_BYTES)
    env["BACKFILL_ENABLED"] = "false"

    subprocess.run(
        ["make", "platform-start"],
        cwd=PROJECT_DIR,
        env=env,
        check=True,
    )


def stop_platform():
    subprocess.run(
        ["make", "platform-stop"],
        cwd=PROJECT_DIR,
        check=True,
    )


def read_run_df(conn, prefix: str) -> pd.DataFrame:
    query = """
            SELECT
                device_id,
                (extract(epoch from humidity_event_time::timestamptz) * 1000)::bigint AS event_ms,
                humidity_ingested_at::bigint AS humidity_ingested_ms,
                temperature_ingested_at::bigint AS temperature_ingested_ms,
                measurement_processing_ts_ms AS measurement_ms,
                enriched_processing_ts_ms AS enriched_ms,
                (extract(epoch from postgres_inserted_at) * 1000)::bigint AS postgres_ms
            FROM sensor_enriched
            WHERE device_id LIKE %(prefix)s
              AND humidity_event_time IS NOT NULL
              AND humidity_ingested_at IS NOT NULL
              AND temperature_ingested_at IS NOT NULL
              AND measurement_processing_ts_ms IS NOT NULL
              AND enriched_processing_ts_ms IS NOT NULL
              AND postgres_inserted_at IS NOT NULL \
            """
    return pd.read_sql(query, conn, params={"prefix": f"{prefix}%"})


def calc_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "samples": 0,
            "mean_create_to_normalized_ms": math.nan,
            "p50_create_to_normalized_ms": math.nan,
            "p95_create_to_normalized_ms": math.nan,
            "mean_create_to_measurement_ms": math.nan,
            "p50_create_to_measurement_ms": math.nan,
            "p95_create_to_measurement_ms": math.nan,
            "mean_create_to_enriched_ms": math.nan,
            "p50_create_to_enriched_ms": math.nan,
            "p95_create_to_enriched_ms": math.nan,
            "mean_create_to_postgres_ms": math.nan,
            "p50_create_to_postgres_ms": math.nan,
            "p95_create_to_postgres_ms": math.nan,
        }

    normalized_ms = df[["humidity_ingested_ms", "temperature_ingested_ms"]].max(axis=1)

    create_to_normalized = normalized_ms - df["event_ms"]
    create_to_measurement = df["measurement_ms"] - df["event_ms"]
    create_to_enriched = df["enriched_ms"] - df["event_ms"]
    create_to_postgres = df["postgres_ms"] - df["event_ms"]

    return {
        "samples": int(len(df)),
        "mean_create_to_normalized_ms": mean(create_to_normalized),
        "p50_create_to_normalized_ms": p(create_to_normalized, 0.50),
        "p95_create_to_normalized_ms": p(create_to_normalized, 0.95),
        "mean_create_to_measurement_ms": mean(create_to_measurement),
        "p50_create_to_measurement_ms": p(create_to_measurement, 0.50),
        "p95_create_to_measurement_ms": p(create_to_measurement, 0.95),
        "mean_create_to_enriched_ms": mean(create_to_enriched),
        "p50_create_to_enriched_ms": p(create_to_enriched, 0.50),
        "p95_create_to_enriched_ms": p(create_to_enriched, 0.95),
        "mean_create_to_postgres_ms": mean(create_to_postgres),
        "p50_create_to_postgres_ms": p(create_to_postgres, 0.50),
        "p95_create_to_postgres_ms": p(create_to_postgres, 0.95),
    }


def append_result(row: dict):
    df = pd.DataFrame([row])
    if RESULTS_CSV.exists():
        old = pd.read_csv(RESULTS_CSV)
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(RESULTS_CSV, index=False)


def main():
    for device_count in DEVICE_COUNTS:
        prefix = f"cnt{device_count}-"
        print(f"\n=== RUN {device_count} devices | prefix={prefix} ===")

        start_platform(device_count, prefix)

        print(f"Collecting data for {RUN_SECONDS}s...")
        time.sleep(RUN_SECONDS)

        conn = psycopg.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )

        try:
            df = read_run_df(conn, prefix)
            metrics = calc_metrics(df)

            result_row = {
                "label": f"{device_count} sensors",
                "prefix": prefix,
                "device_count": device_count,
                "interval_sec": REALTIME_INTERVAL_SEC,
                "payload_bytes": USELESS_PAYLOAD_BYTES,
                "msgs_per_sec": device_count * 4 / REALTIME_INTERVAL_SEC,
                **metrics,
            }

            append_result(result_row)
            print(pd.DataFrame([result_row]).to_string(index=False))

        finally:
            conn.close()

        print("Stopping platform...")
        stop_platform()

    print(f"\nSaved: {RESULTS_CSV}")


if __name__ == "__main__":
    main()