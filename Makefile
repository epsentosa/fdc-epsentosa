run-main:
	poetry run python src/main.py

test:
	poetry run pytest

setup:
	poetry install
	poetry run pre-commit install
