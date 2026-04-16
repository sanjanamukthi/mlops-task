# MLOps Batch Pipeline Task

This project implements a minimal MLOps-style batch pipeline.

## Features

- Config driven (YAML)
- Deterministic runs with seed
- Rolling mean signal generation
- Metrics output
- Structured logging
- Docker support

---

## Local Run

Install dependencies

pip install -r requirements.txt

Run pipeline

python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

---

## Docker Run

Build image

docker build -t mlops-task .

Run container

docker run --rm mlops-task

---

## Output Example

metrics.json

{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.499,
  "latency_ms": 127,
  "seed": 42,
  "status": "success"
}