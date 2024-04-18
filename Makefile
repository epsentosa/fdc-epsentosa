help:
	@echo "Available targets:"
	@echo "  run-main       - Run main.py for ETL"
	@echo "  migration      - Run database migration"
	@echo "  run-clickhouse - Start ClickHouse client"
	@echo "  test           - Run tests"
	@echo "  setup          - Install dependencies, setup pre-commit, and start ClickHouse server"
	@echo "  clean-docker   - Stop and remove ClickHouse Docker container"

run-main:
	poetry run python src/fig_data_challenge/main.py run start-etl

migration:
	poetry run python src/fig_data_challenge/main.py run db-migration

run-clickhouse:
	docker exec -it clickhouse-server clickhouse-client

test:
	poetry run pytest

setup:
	poetry install
	poetry run pre-commit install
	docker build -t clickhouse-server docker/clickhouse
	docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse-server

clean-docker:
	docker stop clickhouse-server
	docker container rm clickhouse-server
	docker image rm clickhouse-server:latest
