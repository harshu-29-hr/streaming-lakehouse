# Shortcuts for running the project locally. Run `make` to see them all.
MONTH  ?= 2025-01
SPEED  ?= 60
ARGS   ?=
PYTHON ?= python3.11
VENV   := .venv
PY     := $(VENV)/bin/python
DATA   := data/yellow_tripdata_$(MONTH).parquet
TOPIC  := taxi.trips.v1

.PHONY: help venv up down reset data profile produce produce-chaos peek

help:  ## list the commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

$(VENV)/.installed: requirements.txt
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt
	@touch $@

venv: $(VENV)/.installed  ## create or update .venv from requirements.txt

up:  ## start Kafka, create the topic, start the UI (http://localhost:8090)
	docker compose up -d

down:  ## stop the containers (Kafka keeps its data)
	docker compose down

reset:  ## wipe Kafka's data and start again with an empty topic
	docker compose down -v
	docker compose up -d

data: $(DATA)  ## download one month of yellow taxi trips (MONTH=YYYY-MM)

$(DATA):
	@mkdir -p data
	curl -fL --progress-bar -o $@ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$(MONTH).parquet

profile: $(VENV)/.installed $(DATA)  ## print a data-quality profile of the month
	$(PY) scripts/explore_trips.py $(DATA)

produce: $(VENV)/.installed $(DATA)  ## replay trips into Kafka (SPEED=60, extra flags in ARGS)
	$(PY) producer/replay_producer.py --file $(DATA) --speed $(SPEED) $(ARGS)

produce-chaos: $(VENV)/.installed $(DATA)  ## same, with 5% late and 2% duplicate deliveries
	$(PY) producer/replay_producer.py --file $(DATA) --speed $(SPEED) --late-rate 0.05 --dup-rate 0.02 $(ARGS)

peek:  ## print 5 events from the topic with their key and partition
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--topic $(TOPIC) --from-beginning --max-messages 5 \
		--formatter-property print.key=true --formatter-property print.partition=true