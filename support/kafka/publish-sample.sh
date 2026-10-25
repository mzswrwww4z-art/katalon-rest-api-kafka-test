#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$project_root"

topic=${KAFKA_TOPIC:-qa.events}
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --topic "$topic" \
  < support/kafka/sample-message.json

printf 'Published sample event from support/kafka/sample-message.json to topic %s\n' "$topic"
