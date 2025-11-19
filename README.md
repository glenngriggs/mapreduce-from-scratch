# MapReduce Engine

Python-based MapReduce framework, inspired by the original MapReduce system described by Google's 2004 paper *MapReduce: Simplified Data Processing on Large Clusters* by Dean & Ghemawat. It follows the logical structure of a Manager–Worker architecture similar to the distributed design used in large-scale data processing systems.

<p align="center">
  <img src="flow.png" width="700">
  <br>
  <em>Execution Diagram with Two Inputs and Two Workers</em>
</p>

## Overview

The framework includes:

- A **Manager** that:
  - Accepts job submissions
  - Splits input into map tasks
  - Assigns tasks to Workers
  - Tracks worker heartbeats and reassigns failed tasks
  - Initiates reduce tasks after maps complete

- **Workers** that:
  - Register with the Manager
  - Execute Map or Reduce tasks using user-supplied executables
  - Produce intermediate key/value files and final reducer outputs

Flow also includes input partitioning, shuffle/sort behavior, and reduce aggregation.

## Project Structure

```
.
├── bin/
│   ├── mapreduce              # Main CLI for starting manager/workers
│   └── faulttol               # Fault tolerance testing helpers
├── mapreduce/
│   ├── submit.py              # Job submission logic
│   ├── manager/
│   │   ├── __main__.py        # Manager entry point
│   │   └── __init__.py
│   ├── worker/
│   │   ├── __main__.py        # Worker entry point
│   │   └── __init__.py
│   └── utils/
│       ├── ordered_dict.py    # Custom deterministic ordered dictionary
│       └── __init__.py
├── tests/
│   ├── test_manager_*.py      # Manager behavior tests
│   ├── test_worker_*.py       # Worker logic + integration tests
│   ├── test_integration_*.py  # End-to-end MR pipeline tests
│   ├── testdata/
│   │   ├── exec/              # Map & Reduce programs (wc, grep)
│   │   ├── input_small/       # Sample map input files
│   │   ├── input_large/       # Stress-testing input
│   │   └── correct/           # Expected outputs
├── var/log/                   # Runtime logs for debugging
├── requirements.txt
├── pyproject.toml
└── README.md
```

## Setup

```bash
python3 -m venv env
source env/bin/activate

pip install -r requirements.txt
pip install -e .
```

## Running the Framework

### Start Manager and Workers

```bash
mapreduce-manager --host localhost --port 6000 --loglevel=INFO
mapreduce-worker --host localhost --port 6001 --manager-host localhost --manager-port 6000
mapreduce-worker --host localhost --port 6002 --manager-host localhost --manager-port 6000
```

### Submit a Job

```bash
mapreduce-submit \
  --input tests/testdata/input_small \
  --output output \
  --mapper tests/testdata/exec/wc_map.sh \
  --reducer tests/testdata/exec/wc_reduce.sh
```
Multiple different map and reduce functions are included,
Outputs will be generated in `output/part-*`.

## Running Tests

```bash
pytest -v
```

## References

- *MapReduce: Simplified Data Processing on Large Clusters*, Jeffrey Dean & Sanjay Ghemawat, OSDI 2004
