RESOURCES := $(shell find ../expyrimenter -name \*.py)

tests:
	PYTHONPATH="$$PYTHON_PATH:.." python3 -m unittest

help:
	@echo '     no target: runs tests target'
	@echo '         tests: run all tests in this folder'
	@echo '      coverage: prints coverage report'
	@echo ' coverage-html: generates report in coverage_html/index.html'
	@echo '         clean: deletes coverage data and html report'

coverage: .coverage FORCE
	coverage3 report

.coverage: test_*.py $(RESOURCES)
	@export PYTHONPATH="$$PYTHON_PATH:.."; \
	for test in test_*.py; do \
		coverage3 run -a $$test; \
	done

coverage-html: .coverage
	coverage3 html

clean:
	rm -rf coverage_html .coverage

FORCE:
