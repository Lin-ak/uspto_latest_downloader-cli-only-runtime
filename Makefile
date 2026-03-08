PYTHON ?= $(shell if [ -x ./.venv/bin/python ]; then printf '%s' ./.venv/bin/python; else printf '%s' python3; fi)
PYTHON_FILES = \
	run_download_latest_once.py \
	core/__init__.py core/common.py core/contract.py core/logging_utils.py core/runtime_security.py \
	sync/__init__.py sync/collaborators.py sync/service.py sync/upstream.py sync/use_case.py sync/zip_utils.py \
	storage/__init__.py storage/job_run_repository.py storage/runtime_cache_repository.py storage/sqlite.py storage/sqlite_connection.py storage/state_repair_service.py storage/state_repository.py storage/status_projection.py

.PHONY: pycompile unittest test run

pycompile:
	$(PYTHON) -m py_compile $(PYTHON_FILES)

unittest:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

test: pycompile unittest

run:
	$(PYTHON) run_download_latest_once.py
