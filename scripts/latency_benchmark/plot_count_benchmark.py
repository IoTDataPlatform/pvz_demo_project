from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "latency_benchmark" / "benchmark-output"
RESULTS_CSV = OUTPUT_DIR / "benchmark_without_analytics.csv"


def plot_latency(
        df: pd.DataFrame,
        col_parse: str,
        col_analytics: str,
        col_save: str,
        ylabel: str,
        title: str,
        output_name: str,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.8))

    x = df["device_count"]

    ax.plot(
        x,
        df[col_parse] / 1000,
        marker="o",
        color="tab:blue",
        linewidth=2,
        label="создание → парсинг",
        )
    ax.plot(
        x,
        df[col_analytics] / 1000,
        marker="o",
        color="tab:orange",
        linewidth=2,
        label="создание → аналитика",
        )
    ax.plot(
        x,
        df[col_save] / 1000,
        marker="o",
        color="tab:green",
        linewidth=2,
        label="создание → сохранение",
        )

    ax.set_title(title, fontsize=20, pad=14)
    ax.set_ylabel(ylabel, fontsize=16)

    ax.set_xlabel("Число датчиков", fontsize=15, labelpad=10)
    ax.set_xticks(x)
    ax.tick_params(axis="x", labelsize=13, pad=6)
    ax.tick_params(axis="y", labelsize=13)

    ax.grid(True, alpha=0.35)
    ax.legend(fontsize=13)

    def devices_to_msgs(v):
        return v * 4

    def msgs_to_devices(v):
        return v / 4

    top_ax = ax.secondary_xaxis(
        "top",
        functions=(devices_to_msgs, msgs_to_devices),
    )
    top_ax.set_xlabel("Число сообщений в секунду", fontsize=15, labelpad=10)
    top_ax.set_xticks(x * 4)
    top_ax.tick_params(axis="x", labelsize=13, pad=6)

    plt.tight_layout()

    out_path = OUTPUT_DIR / output_name
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"Saved: {out_path}")


def main():
    df = pd.read_csv(RESULTS_CSV).sort_values("device_count")

    plot_latency(
        df=df,
        col_parse="mean_create_to_normalized_ms",
        col_analytics="mean_create_to_enriched_ms",
        col_save="mean_create_to_postgres_ms",
        ylabel="Средняя задержка, с",
        title="",
        output_name="latency_vs_device_count_mean_without_analytics.png",
    )

    plot_latency(
        df=df,
        col_parse="p95_create_to_normalized_ms",
        col_analytics="p95_create_to_enriched_ms",
        col_save="p95_create_to_postgres_ms",
        ylabel="p95 задержка, с",
        title="",
        output_name="latency_vs_device_count_p95_without_analytics.png",
    )


if __name__ == "__main__":
    main()