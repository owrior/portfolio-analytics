SHELL=/bin/bash
VENV = venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

setup:
	python3 -m venv $(VENV)
	$(VENV_BIN)/pip install -U pip
	$(VENV_BIN)/pip install -r requirements.txt
	$(VENV_BIN)/pip install -r requirements-lint.txt
	$(VENV_BIN)/pip install pre-commit
	$(VENV_BIN)/pre-commit install
	$(VENV_BIN)/pre-commit run --all-files
