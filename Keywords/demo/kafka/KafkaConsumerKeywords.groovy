package demo.kafka

import com.kms.katalon.core.annotation.Keyword
import groovy.json.JsonSlurper
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Collections
import java.util.Locale
import java.util.Properties
import java.util.concurrent.TimeUnit

class KafkaConsumerKeywords {

    /** Consume JSON records until one contains the configured eventId, or timeout. */
    @Keyword
    Map consumeJsonEvent(
        String bootstrapServers,
        String topic,
        String expectedEventId,
        long timeoutMs,
        String securityProtocol = 'PLAINTEXT',
        String saslMechanism = '',
        String saslJaasConfig = ''
    ) {
        if (!bootstrapServers?.trim()) {
            throw new IllegalArgumentException('Kafka bootstrap servers must not be empty.')
        }
        if (!topic?.trim()) {
            throw new IllegalArgumentException('Kafka topic must not be empty.')
        }
        if (!expectedEventId?.trim()) {
            throw new IllegalArgumentException('Expected eventId must not be empty.')
        }
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException('Kafka poll timeout must be greater than zero.')
        }

        Properties properties = new Properties()
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "katalon-demo-${UUID.randomUUID()}")
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.name)
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.name)
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, 'false')
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol)

        if (securityProtocol?.toUpperCase(Locale.ROOT)?.startsWith('SASL')) {
            if (!saslMechanism?.trim() || !saslJaasConfig?.trim()) {
                throw new IllegalArgumentException(
                    'SASL protocol requires KAFKA_SASL_MECHANISM and KAFKA_SASL_JAAS_CONFIG.'
                )
            }
            properties.put('sasl.mechanism', saslMechanism)
            properties.put('sasl.jaas.config', saslJaasConfig)
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        consumer.subscribe(Collections.singletonList(topic))

        try {
            while (System.nanoTime() < deadline) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())
                long pollMs = Math.max(1L, Math.min(250L, remainingMs))
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMs))

                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null) {
                        continue
                    }
                    try {
                        Object parsed = new JsonSlurper().parseText(record.value())
                        if (parsed instanceof Map && parsed.eventId?.toString() == expectedEventId) {
                            return parsed as Map
                        }
                    } catch (Exception ignoredMalformedRecord) {
                        // Ignore unrelated non-JSON records on a shared topic.
                    }
                }
            }
            throw new IllegalStateException(
                "No Kafka JSON event with eventId '${expectedEventId}' arrived on '${topic}' within ${timeoutMs} ms."
            )
        } finally {
            consumer.close()
        }
    }
}
