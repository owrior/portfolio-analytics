# portfolio-analytics

Module for tracking portfolio performance.

## Installing

### Python
python -m venv .venv
source .venv/bin/activate
pip install poetry
poetry install

### Docker
Basic postgresql DB.

## Runnning CI tools

isort --profile black pfa tests; black pfa tests; flake8 pfa tests --count --exit-zero --max-complexity=10 --max-line-length=88 --statistics

## Running workflows

python3 -m pfa -wf initialise; python3 -m pfa -wf validation; python3 -m pfa -wf forecast
