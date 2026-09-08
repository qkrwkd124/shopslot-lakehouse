COMPOSE := docker compose

.PHONY: up down reset migrate connector generate-p0 verify-p0 smoke logs ps api

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
