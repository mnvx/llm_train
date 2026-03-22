PYTHON  = .venv/bin/python
PIP     = .venv/bin/pip

.DEFAULT_GOAL := help

.PHONY: help install fetch validate clean

help:
	@echo ""
	@echo "  make install          — create .venv and install dependencies"
	@echo "  make fetch            — fetch last trading day"
	@echo "  make fetch DATE=...   — fetch specific date (YYYY-MM-DD)"
	@echo "  make validate         — validate all data files"
	@echo "  make clean            — remove .venv"
	@echo ""

install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo ""
	@echo "Done. No need to activate — just use 'make fetch' etc."

fetch:
ifdef DATE
	$(PYTHON) fetch.py --date $(DATE)
else
	$(PYTHON) fetch.py
endif

validate:
	$(PYTHON) validate.py

clean:
	rm -rf .venv
