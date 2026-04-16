import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys


def load_config(config_path):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    required_keys = ["seed", "window", "version"]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    return config


def load_dataset(path):
    try:
        df = pd.read_csv(path)
    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("CSV file is empty")

    if "close" not in df.columns:
        raise ValueError("Missing required column: close")

    return df


def compute_signal(df, window):
    df["rolling_mean"] = df["close"].rolling(window).mean()
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
    return df


def write_metrics(output_file, metrics):
    with open(output_file, "w") as f:
        json.dump(metrics, f, indent=4)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    start_time = time.time()

    try:

        logging.info("Job started")

        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        np.random.seed(seed)

        df = load_dataset(args.input)

        logging.info(f"Rows loaded: {len(df)}")

        df = compute_signal(df, window)

        logging.info("Rolling mean and signals computed")

        rows_processed = len(df)
        signal_rate = float(df["signal"].mean())

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success",
        }

        write_metrics(args.output, metrics)

        logging.info(f"Metrics: {metrics}")
        logging.info("Job finished successfully")

        print(json.dumps(metrics))

    except Exception as e:

        logging.error(str(e))

        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e),
        }

        write_metrics(args.output, error_metrics)

        print(json.dumps(error_metrics))

        sys.exit(1)


if __name__ == "__main__":
    main()