# portfolio-analytics

Module for tracking portfolio performance.

## Installing

### Python setup

```
python -m venv .venv
source .venv/bin/activate
pip install poetry
poetry install
```

### Docker setup

Basic postgresql DB.

### Pre-commit hooks (for Dev)

```
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

## Running workflows

```
prefect run -p pfa/workflows/initialise.py
```
