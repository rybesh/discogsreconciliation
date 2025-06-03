SHELL := /bin/bash
PYTHON := ./venv/bin/python
PIP := ./venv/bin/python -m pip
.DEFAULT_GOAL := run

$(PYTHON):
	python3 -m venv venv
	$(PIP) install --upgrade pip
	$(PIP) install wheel
	$(PIP) install -r requirements.txt

clean:
	rm -rf venv

run: | $(PYTHON)
	$(PYTHON) discogs.py

.PHONY: clean run
