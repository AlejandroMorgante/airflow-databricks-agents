.PHONY: init setup build up up-local down logs password check check-agent check-setup e2e refresh refresh-llm-token

export DATABRICKS_PROFILE DATABRICKS_APP_NAME DATABRICKS_MODEL_ENDPOINT

init:
	@test -f .env || cp .env.example .env

build: init
	docker compose build

setup: init
	python3 scripts/bootstrap_databricks.py

up: setup
	docker compose up --build -d --wait --wait-timeout 600

up-local: init
	docker compose up --build -d --wait --wait-timeout 600

down:
	docker compose down

logs:
	docker compose logs -f airflow

password:
	docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated

refresh-llm-token:
	python3 scripts/refresh_databricks_llm_token.py

check-agent:
	.venv/bin/python scripts/check_agent.py

check-setup:
	python3 -m unittest discover -s tests -p 'test_setup.py' -v

check: build
	docker compose run --rm --no-deps -e CONNECTION_CHECK_MAX_COUNT=0 airflow python /opt/airflow-tools/check_dags.py

e2e: up
	docker compose exec -T airflow python /opt/airflow-tools/check_dags.py
	docker compose exec -T airflow python /opt/airflow-tools/run_end_to_end.py $(E2E_ARGS)

# main moves, but a cached Docker RUN layer does not. Refresh it explicitly.
refresh: setup
	docker compose build --no-cache --pull
	docker compose up -d --wait --wait-timeout 600
