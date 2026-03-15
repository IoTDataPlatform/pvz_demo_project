from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "latency_benchmark" / "benchmark-output"
RESULTS_CSV = OUTPUT_DIR / "count_benchmark_metrics.csv"


def main():
    df = pd.read_csv(RESULTS_CSV).sort_values("device_count")

    plt.figure(figsize=(10, 6))
    plt.plot(df["device_count"], df["p95_create_to_normalized_ms"] / 1000, marker="o", label="emulator → kafka")
    plt.plot(df["device_count"], df["p95_create_to_enriched_ms"] / 1000, marker="o", label="emulator → flink")
    plt.plot(df["device_count"], df["p95_create_to_postgres_ms"] / 1000, marker="o", label="emulator → postgres")

    plt.xlabel("число датчиков")
    plt.ylabel("p95 задержка, с")
    plt.title("Задержка vs число датчиков")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "latency_vs_device_count.png", dpi=150)
    plt.close()

    print("Saved:", OUTPUT_DIR / "latency_vs_device_count.png")


if __name__ == "__main__":
    main()