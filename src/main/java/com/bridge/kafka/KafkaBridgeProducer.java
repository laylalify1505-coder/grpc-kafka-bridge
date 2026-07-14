package com.bridge.kafka;

import com.bridge.config.BridgeConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import subserver.dataplane.v1.DataEnvelope;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serializes each DataEnvelope to JSON and publishes to the configured Kafka topic.
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

    /**
     * Convert a protobuf DataEnvelope to a flat JSON map and send to Kafka.
     */
    public void sendToKafka(DataEnvelope envelope) {
        try {
            Map<String, Object> msg = new LinkedHashMap<>();
            msg.put("correlation_id", envelope.getCorrelationId());
            msg.put("iot_id", envelope.getIotId());
            msg.put("data_type", envelope.getDataType());
            msg.put("data", envelope.getData());
            msg.put("ts", System.currentTimeMillis());

            String json = mapper.writeValueAsString(msg);

            kafkaTemplate.send(config.getKafka().getTopic(), json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send to Kafka topic [{}]: {}",
                                    config.getKafka().getTopic(), ex.getMessage());
                        }
                    });

        } catch (Exception e) {
            log.error("Failed to serialize DataEnvelope: {}", e.getMessage());
        }
    }
}
