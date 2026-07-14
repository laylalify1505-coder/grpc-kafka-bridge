package com.bridge.kafka;

import com.bridge.config.BridgeConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Sends a standardised envelope to Kafka:
 * { "type": "...", "source": "...", "version": "...", "payload": { ... } }
 */
@Service
public class KafkaBridgeProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaBridgeProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BridgeConfig config;
    private final ObjectMapper mapper;

    public KafkaBridgeProducer(KafkaTemplate<String, String> kafkaTemplate,
                               BridgeConfig config) {
        this.kafkaTemplate = kafkaTemplate;
        this.config = config;
        this.mapper = new ObjectMapper();
    }

    public void sendToKafka(String correlationId, long iotId, String dataType, String data) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("correlation_id", correlationId);
            payload.put("iot_id", iotId);
            payload.put("data_type", dataType);
            payload.put("data", data);
            payload.put("received_at", System.currentTimeMillis());

            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("type", config.getKafka().getType());
            envelope.put("source", config.getKafka().getSource());
            envelope.put("version", config.getKafka().getVersion());
            envelope.put("payload", payload);

            String json = mapper.writeValueAsString(envelope);

            kafkaTemplate.send(config.getKafka().getTopic(), json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send to Kafka topic [{}]: {}",
                                    config.getKafka().getTopic(), ex.getMessage());
                        }
                    });

        } catch (Exception e) {
            log.error("Failed to serialize message: {}", e.getMessage());
        }
    }
}
