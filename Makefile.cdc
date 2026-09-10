# Phase 2 targets. Append these to your existing Makefile, or keep this file
# and run them with:  make -f Makefile.cdc <target>

RPK = docker exec -it cdc-redpanda rpk

.PHONY: cdc-up cdc-down cdc-nuke cdc-status cdc-register cdc-connector cdc-drop cdc-test \
        cdc-consume cdc-consume-once cdc-consume-sf cdc-dlq cdc-lag cdc-reconcile \
        cdc-topics cdc-watch-orders cdc-watch-customers cdc-load cdc-load-once \
        cdc-psql cdc-slot cdc-logs cdc-chaos cdc-load-sf cdc-dbt cdc-dbt-docs

## --- stack ------------------------------------------------------------------

cdc-up:            ## start postgres, redpanda, console, connect
	docker compose up -d
	@echo "waiting for Kafka Connect to answer..."
	@until curl -sf localhost:8083/ >/dev/null; do sleep 2; done
	@echo "connect is up. console: http://localhost:8080"

cdc-down:          ## stop containers, keep the data
	docker compose down

cdc-nuke:          ## stop and delete volumes (postgres data, kafka topics, offsets)
	docker compose down -v

cdc-status:        ## container + connector state
	docker compose ps
	@echo
	@curl -s localhost:8083/connectors/orders-connector/status | python3 -m json.tool || true

cdc-logs:
	docker compose logs -f connect

## --- connector --------------------------------------------------------------

cdc-register:      ## register the Debezium Postgres connector
	curl -s -X POST -H "Content-Type: application/json" \
	  --data @cdc/connectors/postgres-orders.json \
	  localhost:8083/connectors | python3 -m json.tool

cdc-connector:     ## show connector config as Connect actually holds it
	@curl -s localhost:8083/connectors/orders-connector/config | python3 -m json.tool

cdc-drop:          ## delete the connector (leaves the replication slot behind!)
	curl -s -X DELETE localhost:8083/connectors/orders-connector

## --- looking at the stream ---------------------------------------------------

cdc-topics:
	$(RPK) topic list

cdc-watch-orders:  ## tail change events for the orders table
	$(RPK) topic consume ordersdb.public.orders

cdc-watch-customers:
	$(RPK) topic consume ordersdb.public.customers

## --- the application ---------------------------------------------------------

cdc-load:          ## continuous write traffic (ctrl-c to stop)
	python cdc/generate_load.py

cdc-load-once:     ## exactly 20 changes, then stop
	python cdc/generate_load.py --n 20 --sleep 0.2

cdc-psql:          ## psql shell against the app database
	docker exec -it cdc-postgres psql -U orders -d ordersdb

cdc-slot:          ## replication slot state - watch restart_lsn move
	docker exec -it cdc-postgres psql -U orders -d ordersdb -c \
	  "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, \
	          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal \
	   FROM pg_replication_slots;"

## --- consumer ----------------------------------------------------------------

cdc-test:          ## parser unit tests, no Kafka, no warehouse
	python -m pytest tests/test_cdc_parse.py -q

cdc-consume:       ## tail the topics into the local DuckDB warehouse
	python cdc/consume_changes.py --target duckdb

cdc-consume-once:  ## drain whatever is queued, then exit
	python cdc/consume_changes.py --target duckdb --once

cdc-consume-sf:    ## same, into Snowflake RAW.CDC_EVENTS
	python cdc/consume_changes.py --target snowflake

cdc-dlq:           ## read the dead-letter topic
	$(RPK) topic consume cdc.dlq

cdc-lag:           ## how far behind the consumer group is
	$(RPK) group describe $(or $(CDC_GROUP_ID),cdc-warehouse-loader)

cdc-reconcile:     ## does the warehouse agree with postgres?
	python cdc/reconcile.py

cdc-chaos:         ## kill the consumer mid-stream, assert no loss / no dupes
	python cdc/chaos_test.py --kills 3

## --- modelling ---------------------------------------------------------------

cdc-load-sf:       ## land the change events in Snowflake RAW.CDC_EVENTS
	python cdc/consume_changes.py --target snowflake --once

cdc-dbt:           ## build + test every CDC model
	cd dbt && dbt build --select tag:cdc

cdc-dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve
