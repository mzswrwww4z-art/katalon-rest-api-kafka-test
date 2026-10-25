# Katalon REST API and Kafka demo

This Katalon Studio project was prepared for the HackerRank brief shown in the attached screenshot. It contains one REST flow where Katalon creates a resource with `POST` and reads it back with `GET`, plus a Kafka flow where Katalon consumes and validates a JSON event.

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
```

## What the tests do

### REST API: producer and consumer

The `REST Producer and Consumer` test case builds requests with Katalon’s `RequestObject` and sends them using Katalon’s `WS.sendRequest` keyword:

1. `POST /api/messages` creates a message and returns its generated ID.
2. `GET /api/messages/{id}` reads the saved message.
3. The test checks status codes, IDs, event type, and payload.

The small Node.js API under `support/api` is the local system under test. Its data is held in memory and is cleared when the service restarts.

### Kafka: consumer

The `Consume Event` test uses Apache’s Java Kafka client from a Katalon custom keyword. It subscribes to the configured topic and polls for a JSON record whose `eventId` matches the configured value. The poll has a timeout, uses a unique consumer group, and does not commit offsets.

## Requirements

- Katalon Studio with a supported Java runtime (Studio 9/10 use Java 17; Studio 11 uses Java 21)
- Docker Desktop or Docker Engine with Docker Compose
- Network access the first time Gradle downloads the Kafka client and Docker downloads the service images

The sample runs a local Kafka broker in plaintext mode bound to `127.0.0.1`. This is for a local demonstration only; do not expose this broker to an untrusted network.

## Setup and run

1. Open `KatalonApiKafkaDemo.prj` in Katalon Studio. If Studio asks to trust the project, review the files and trust the project before execution.
2. Resolve the Kafka Java client dependency. In Katalon Studio, open the **Gradle Tasks** view and run `katalonCopyDependencies` for this project. If using a system Gradle installation instead, run `gradle katalonCopyDependencies` from the project folder. The task copies the Kafka client and its required libraries into `Drivers/`; those generated JAR files are ignored by Git. After it completes, select **Project > Refresh** so Katalon reloads the classpath.
3. Start the local services from this folder:

   ```sh
   docker compose up -d
   ```

4. Wait until the REST API is ready:

   ```sh
   curl --fail http://127.0.0.1:8080/health
   ```

   It should return `{"status":"ok"}`. Also wait for the Kafka container to become healthy, then create the sample topic:

   ```sh
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server 127.0.0.1:9092 \
     --create --if-not-exists --topic qa.events \
     --partitions 1 --replication-factor 1
   ```

5. Publish the sample event before running the Katalon suite:

   ```sh
   ./support/kafka/publish-sample.sh
   ```

   The sample event ID is `demo-event-001`. If you edit `support/kafka/sample-message.json`, update the matching Katalon profile value or environment variable too.

6. In Katalon Studio, open **Test Suites > Integration Demo** and run it with the `default` execution profile. The REST case runs first; the Kafka case follows.

To stop the local services:

```sh
docker compose down
```

## Safe configuration

The committed profile contains local demo endpoints and a non-secret sample event ID. Environment variables override the values shown below:

| Variable | Default | Purpose |
| --- | --- | --- |
| `API_BASE_URL` | `http://127.0.0.1:8080` | REST demo base URL |
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `qa.events` | Topic to consume; the sample producer also accepts this variable |
| `KAFKA_EXPECTED_EVENT_ID` | `demo-event-001` | JSON `eventId` expected by the consumer test |
| `KAFKA_POLL_TIMEOUT_MS` | `10000` | Maximum wait for the matching Kafka message |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Set to the protocol required by a secured broker, such as `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | empty | Required for a SASL protocol |
| `KAFKA_SASL_JAAS_CONFIG` | empty | Required for a SASL protocol; supply it through the local environment or your CI secret store |

For a secured Kafka service, launch Katalon from an environment that has the required variables set. Keep passwords, tokens, JAAS strings, private keys, and real customer data out of the project files and Git history. The demo does not require any credentials.

## Troubleshooting

- **REST connection refused:** check `docker compose ps` and confirm `http://127.0.0.1:8080/health` responds with `{"status":"ok"}`.
- **Kafka connection refused:** wait until `docker compose ps` reports the broker healthy and confirm the topic exists with `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list`.
- **Kafka event timeout:** publish the sample after creating the topic, and ensure `KAFKA_EXPECTED_EVENT_ID` matches the event JSON exactly.
- **Kafka classes cannot be resolved:** run `katalonCopyDependencies`, then refresh/reopen the project so Studio reloads `Drivers/`.

## Publish a public repository

The HackerRank brief asks for a public GitHub or GitLab repository. This project is prepared locally; it has no remote URL yet.

### GitHub

1. On GitHub, create a new repository and set visibility to **Public**. Leave repository initialization options such as README unchecked, since this folder already contains the project files.
2. From this project folder, initialize Git and make the first commit:

   ```sh
   git init
   git add .
   git status
   git commit -m "Add Katalon REST and Kafka integration demo"
   git branch -M main
   ```

3. Copy the real HTTPS or SSH remote URL shown by GitHub and add it, then push:

   ```sh
   git remote add origin <[katalon-rest-api-kafka-test](https://github.com/mzswrwww4z-art/katalon-rest-api-kafka-test)>
   git push -u origin main
   ```

### GitLab

Create a **Public** blank project, then use its real clone URL in the same `git remote add origin ...` and `git push -u origin main` steps.

Before submitting, confirm that the repository opens while signed out (or in a private browser window), and that the Katalon project file, test cases, scripts, setup files, and README are visible. The repository URL must be copied from the hosting service after you create it; no URL is included here because none exists yet.

In the HackerRank answer box, paste the public repository URL after the push completes.
