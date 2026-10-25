# Katalon REST API and Kafka Integration Test

This Katalon Studio project demonstrates the two scenarios requested in the HackerRank technical test:

1. Katalon acts as a REST API producer and consumer: it creates a message with `POST`, reads it with `GET`, and validates the response.
2. Katalon acts as a Kafka consumer: it polls a topic and validates a JSON event.

The project includes a small local REST API and a Docker-based Kafka broker so the scenarios have a reproducible local environment.

**Public repository:** [katalon-rest-api-kafka-test](https://github.com/mzswrwww4z-art/katalon-rest-api-kafka-test)

## Assignment coverage

| Requirement | Project implementation |
| --- | --- |
| RESTful API producer and consumer | The REST Producer and Consumer test sends `POST /api/messages`, then `GET /api/messages/{id}`, and checks the status codes and returned data. |
| Kafka consumer | The Consume Event test uses a custom Katalon keyword backed by the Apache Kafka Java client. It waits for a JSON event with the expected `eventId`. |
| Public source repository | This project is available in the public GitHub repository linked above. |

## Project structure

```text
KatalonApiKafkaDemo.prj
Test Cases/
  REST API/REST Producer and Consumer.tc
  Kafka/Consume Event.tc
Scripts/Test Cases/
  REST API/REST Producer and Consumer.groovy
  Kafka/Consume Event.groovy
Keywords/demo/kafka/KafkaConsumerKeywords.groovy
Test Suites/Integration Demo.ts
Profiles/default.glbl
support/api/server.js
support/kafka/sample-message.json
support/kafka/publish-sample.sh
docker-compose.yml
build.gradle
README.md
```

## Requirements

- Katalon Studio with the Java runtime supported by your Studio version. Studio 9/10 use Java 17; Studio 11 uses Java 21.
- Docker Desktop or Docker Engine with Docker Compose.
- Internet access the first time Gradle downloads the Kafka client and Docker downloads the service images.

The local Kafka broker uses plaintext and binds to `127.0.0.1`. It is intended for local demonstration only; do not expose it to an untrusted network.

## Setup and run

Run these steps from the project folder.

1. Open `KatalonApiKafkaDemo.prj` in Katalon Studio. Review the project files before trusting the project if Studio prompts you.
2. Resolve the Kafka Java client dependency. In Katalon Studio, open **Gradle Tasks** and run `katalonCopyDependencies`. When it completes, select **Project > Refresh** so Katalon reloads the classpath. If you use system Gradle, run `gradle katalonCopyDependencies` from the project folder instead. Generated JAR files in `Drivers/` are ignored by Git.
3. Start the local services:

   ```sh
   docker compose up -d
   ```

4. Wait until the REST API responds successfully:

   ```sh
   curl --fail http://127.0.0.1:8080/health
   ```

   The response should be `{"status":"ok"}`. Check `docker compose ps` and wait until Kafka reports healthy.

5. Create the Kafka topic:

   ```sh
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server 127.0.0.1:9092 \
     --create --if-not-exists --topic qa.events \
     --partitions 1 --replication-factor 1
   ```

6. Publish the sample event before running the Katalon suite:

   ```sh
   ./support/kafka/publish-sample.sh
   ```

   The sample event ID is `demo-event-001`. If you change `support/kafka/sample-message.json`, update the expected ID in the Katalon execution profile or environment variable too.

7. In Katalon Studio, open **Test Suites > Integration Demo** and run it with the `default` execution profile. The REST test case runs first, followed by the Kafka consumer test case.

Stop the local services when you are finished:

```sh
docker compose down
```

## Configuration

The default profile uses local demo services and non-secret sample data. Environment variables override the corresponding values.

| Variable | Default | Purpose |
| --- | --- | --- |
| `API_BASE_URL` | `http://127.0.0.1:8080` | Base URL for the local REST API |
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `qa.events` | Topic consumed by Katalon and used by the sample publisher |
| `KAFKA_EXPECTED_EVENT_ID` | `demo-event-001` | Event ID expected by the Kafka test |
| `KAFKA_POLL_TIMEOUT_MS` | `10000` | Maximum time the consumer waits for the event |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Kafka security protocol |
| `KAFKA_SASL_MECHANISM` | empty | SASL mechanism when using a SASL-secured broker |
| `KAFKA_SASL_JAAS_CONFIG` | empty | SASL JAAS configuration when using a SASL-secured broker |

The demo requires no credentials. Keep passwords, tokens, JAAS strings, private keys, and real customer data out of the project files and GitHub. The `.gitignore` excludes `.env` files, reports, and generated dependency JARs.

## Notes

- The sample REST API keeps records in memory. Its data is cleared when the API service restarts.
- The Kafka sample event is published separately before the test suite runs; Katalon remains the Kafka consumer in this scenario.
