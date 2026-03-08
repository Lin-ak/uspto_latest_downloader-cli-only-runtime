PYTHON ?= $(shell if [ -x ./.venv/bin/python ]; then printf '%s' ./.venv/bin/python; else printf '%s' python3; fi)
PYTHON_FILES = api_contract.py api_http.py api_paths.py api_routes_public.py api_schemas.py app_factory.py downloader.py downloader_common.py downloader_service.py downloader_storage.py downloader_upstream.py downloader_zip.py logging_utils.py run_download_latest_once.py server.py test_downloader.py tests/__init__.py tests/common.py tests/test_api.py tests/test_service.py tests/test_storage.py

.PHONY: pycompile unittest test ci run

pycompile:
	$(PYTHON) -m py_compile $(PYTHON_FILES)

unittest:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

test: pycompile unittest

ci: test

run:
	$(PYTHON) server.py --host 127.0.0.1 --port 8010
