PYTHON ?= $(shell if [ -x ./.venv/bin/python ]; then printf '%s' ./.venv/bin/python; else printf '%s' python3; fi)
PYTHON_FILES = \
	run_download_latest_once.py \
	core/__init__.py core/common.py core/contract.py core/logging_utils.py \
	sync/__init__.py sync/service.py sync/upstream.py sync/zip_utils.py \
	storage/__init__.py storage/sqlite.py

.PHONY: pycompile run

pycompile:
	$(PYTHON) -m py_compile $(PYTHON_FILES)

run:
	$(PYTHON) run_download_latest_once.py
