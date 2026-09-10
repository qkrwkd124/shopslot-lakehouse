COMPOSE := docker compose

.PHONY: up down reset migrate connector generate-p0 verify-p0 smoke logs ps api
.PHONY: iceberg-up iceberg-demo iceberg-sql
.PHONY: bronze-once bronze-up bronze-stop bronze-logs verify-bronze

bronze-once:
	$(COMPOSE) exec -T spark python3 /opt/spark/work-dir/spark/run.py submit /opt/spark/work-dir/spark/jobs/bronze_stream.py --available-now

bronze-up:
	$(COMPOSE) --profile streaming up -d --build bronze

bronze-stop:
	$(COMPOSE) stop bronze

bronze-logs:
	$(COMPOSE) logs --follow --tail=80 bronze

verify-bronze:
	bash scripts/verify_bronze.sh

iceberg-up:
	$(COMPOSE) up -d --build spark

iceberg-demo:
	$(COMPOSE) exec -T spark python3 /opt/spark/work-dir/spark/run.py submit /opt/spark/work-dir/spark/jobs/iceberg_demo.py

iceberg-sql:
	$(COMPOSE) exec spark python3 /opt/spark/work-dir/spark/run.py sql

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down --remove-orphans

reset:
	$(COMPOSE) down -v --remove-orphans

migrate:
	$(COMPOSE) run --build --rm migrate

connector:
	./debezium/register-connector.sh

generate-p0:
	$(COMPOSE) --profile tools run --build --rm generator python produce_p0_events.py

verify-p0:
	./scripts/verify_p0.sh

smoke: reset up connector generate-p0 verify-p0

logs:
	$(COMPOSE) logs --follow --tail=100

ps:
	$(COMPOSE) ps

api:
	$(COMPOSE) --profile api up -d api
