COMPOSE := docker compose
.DEFAULT_GOAL := up

.PHONY: add-booking
.PHONY: up down reset migrate connector generate-p0 verify-p0 smoke logs ps api
.PHONY: iceberg-up iceberg-demo iceberg-sql
.PHONY: thrift-up thrift-stop thrift-logs
.PHONY: bronze-once bronze-up bronze-stop bronze-logs verify-bronze
.PHONY: silver-events verify-silver-events silver-current

add-booking:
	$(COMPOSE) --profile tools run --build --rm generator python add_booking.py

thrift-up:
	$(COMPOSE) up -d --build spark-thrift

thrift-stop:
	$(COMPOSE) stop spark-thrift

thrift-logs:
	$(COMPOSE) logs --follow --tail=80 spark-thrift

silver-current:
	$(COMPOSE) exec -T spark python3 /opt/spark/work-dir/spark/run.py submit /opt/spark/work-dir/spark/jobs/silver_bookings_current.py

silver-events:
	$(COMPOSE) exec -T spark python3 /opt/spark/work-dir/spark/run.py submit /opt/spark/work-dir/spark/jobs/silver_booking_events.py

verify-silver-events:
	$(COMPOSE) exec -T spark python3 /opt/spark/work-dir/spark/run.py submit /opt/spark/work-dir/spark/jobs/silver_booking_events.py --self-test

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
