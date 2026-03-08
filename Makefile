PYTHON ?= $(shell if [ -x ./.venv/bin/python ]; then printf '%s' ./.venv/bin/python; else printf '%s' python3; fi)
PYTHON_FILES = \
	run_download_latest_once.py server.py \
	app/__init__.py app/factory.py app/http.py app/paths.py app/routes_public.py app/schemas.py \
	core/__init__.py core/common.py core/contract.py core/logging_utils.py \
	sync/__init__.py sync/service.py sync/upstream.py sync/zip_utils.py \
	storage/__init__.py storage/sqlite.py \
	test_downloader.py tests/__init__.py tests/common.py tests/test_api.py tests/test_service.py tests/test_storage.py

.PHONY: pycompile unittest test ci run

pycompile:
	$(PYTHON) -m py_compile $(PYTHON_FILES)

unittest:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

test: pycompile unittest

ci: test

run:
	$(PYTHON) server.py --host 127.0.0.1 --port 8010
