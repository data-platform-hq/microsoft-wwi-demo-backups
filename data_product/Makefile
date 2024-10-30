include .env

# .PHONY: start build stop

default: up
start: up
show: state
ps: state
status: state
run: exec

up:
	@echo "Starting $(COMPOSE_PROJECT_NAME)..."
	docker compose up -d

build:
	@echo "Build and start $(COMPOSE_PROJECT_NAME)..."
	docker compose up -d --build

stop:
	@echo "Stop $(COMPOSE_PROJECT_NAME)..."
	docker compose stop

down:
	@echo "Down $(COMPOSE_PROJECT_NAME) and remove resources"
	docker compose down -v

state:
	@echo "Show running containers"
	docker compose ps

exec:
	@echo "Exec container $(COMPOSE_PROJECT_NAME) and bash"
	docker exec -ti $(COMPOSE_PROJECT_NAME)-mssql bash

restore: up
	@echo "Restore WWI data to MSSQL db"
	docker exec -ti $(COMPOSE_PROJECT_NAME)-mssql /opt/mssql-tools/bin/sqlcmd \
		  -U $(MSSQL_USER_NAME) \
		  -P $(MSSQL_PASSWORD) \
		  -i /usr/sqlscripts/init_product_db.sql \
          -v  MSSQL_DB_NAME=$(MSSQL_DB_NAME) MSSQL_DIMENSIONS_SCHEMA=$(MSSQL_DIMENSIONS_SCHEMA)  MSSQL_FACT_SCHEMA=$(MSSQL_FACT_SCHEMA)

%:
	@:
