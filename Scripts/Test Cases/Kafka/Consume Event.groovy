import internal.GlobalVariable as GlobalVariable
import com.kms.katalon.core.util.KeywordUtil

String bootstrapServers = System.getenv('KAFKA_BOOTSTRAP_SERVERS') ?: GlobalVariable.kafkaBootstrapServers
String topic = System.getenv('KAFKA_TOPIC') ?: GlobalVariable.kafkaTopic
String expectedEventId = System.getenv('KAFKA_EXPECTED_EVENT_ID') ?: GlobalVariable.kafkaExpectedEventId
String timeoutSetting = System.getenv('KAFKA_POLL_TIMEOUT_MS') ?: GlobalVariable.kafkaPollTimeoutMs.toString()
long timeoutMs = Long.parseLong(timeoutSetting)

String securityProtocol = System.getenv('KAFKA_SECURITY_PROTOCOL') ?: 'PLAINTEXT'
String saslMechanism = System.getenv('KAFKA_SASL_MECHANISM') ?: ''
String saslJaasConfig = System.getenv('KAFKA_SASL_JAAS_CONFIG') ?: ''

Map event = CustomKeywords.'demo.kafka.KafkaConsumerKeywords.consumeJsonEvent'(
    bootstrapServers,
    topic,
    expectedEventId,
    timeoutMs,
    securityProtocol,
    saslMechanism,
    saslJaasConfig
)

assert event.eventId?.toString() == expectedEventId : "Expected eventId '${expectedEventId}', received '${event.eventId}'."
assert event.eventType : 'Consumed event did not include eventType.'
KeywordUtil.logInfo("Kafka consumer received eventId=${expectedEventId} from topic=${topic}")
