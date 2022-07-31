.PHONY: all
all: test lint

.PHONY: test
test:
	poetry run pytest -v tests

.PHONY: lint
lint:
	poetry run pyflakes src/dtl tests
	poetry run pylint -E src/dtl tests
	poetry run mypy src/dtl
	poetry run black --check src/dtl tests
	poetry run isort --check-only src/dtl tests
	poetry run ssort --check --diff src/dtl tests

.PHONY: format
format:
	poetry run ssort src/dtl/ tests/
	poetry run isort src/dtl/ tests/
	poetry run black src/dtl/ tests/
