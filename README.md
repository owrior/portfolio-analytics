# portfolio-analytics

Module for tracking portfolio performance.

## Installing requirements

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Runnning CI tools

isort --profile black pfa tests & black pfa tests & flake8 pfa tests --count --exit-zero --max-complexity=10 --max-line-length=88 --statistics
