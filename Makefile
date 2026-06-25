# kit4go-verify: end-to-end verification for github.com/v8fg/kit4go/log4go.
#
# Targets:
#   kafka-up    start a single-node Kafka (KRaft) on :9092 via docker compose
#   kafka-down  stop and remove the Kafka container + volume
#   kafka-ready block until Kafka answers `kafka-topics --list`
#   run         go run . against the local Kafka (all sections)
#   run-no-kafka  go run . --no-kafka (skips the Kafka section)
#   verify      kafka-up + wait ready + run (full e2e) + leave Kafka up
#   clean       remove generated verify logs and the built binary
#
# Env:
#   KAFKA_BROKERS  comma-separated broker list (default: localhost:9092)

BROKERS    ?= localhost:9092
GOFLAGS    ?=
BIN        := kit4go-verify

.PHONY: kafka-up kafka-down kafka-ready run run-no-kafka verify clean build fmt vet

build:
	go build $(GOFLAGS) -o $(BIN) .

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down -v

# Block until the broker lists topics (ready for produce/consume).
kafka-ready:
	@echo "waiting for kafka to be ready..."
	@for i in $$(seq 1 40); do \
		if docker exec kit4go-verify-kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
			--bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
			echo "kafka ready"; exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "kafka not ready after 80s"; exit 1

run: build
	KAFKA_BROKERS=$(BROKERS) ./$(BIN)

run-no-kafka: build
	./$(BIN) --no-kafka

# Full e2e: bring up Kafka, wait for it, run all sections. Kafka is left
# running so messages can be inspected afterwards; use `make kafka-down` to clean.
verify: kafka-up kafka-ready run

clean:
	rm -f $(BIN)
	rm -f /tmp/kit4go-verify-*.log /tmp/fwtest-*.log /tmp/diag-*.log /tmp/diag2-*.log

fmt:
	go fmt ./...

vet:
	go vet ./...
