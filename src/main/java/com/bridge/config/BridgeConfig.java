package com.bridge.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * All values resolved from environment variables with sensible defaults.
 */
@Component
public class BridgeConfig {

    // ── gRPC ────────────────────────────────────────────────────────────────
    @Value("${SUB_SERVER_HOST:localhost}")
    private String grpcHost;

    @Value("${SUB_SERVER_PORT:50051}")
    private int grpcPort;

    /** 0 = subscribe to all devices */
    @Value("${IOT_ID:0}")
    private long grpcIotId;

    @Value("${RECONNECT_DELAY_MS:5000}")
    private int grpcReconnectDelayMs;

    // ── Kafka ───────────────────────────────────────────────────────────────
    @Value("${KAFKA_TOPIC:subserver-data}")
    private String kafkaTopic;

    @Value("${KAFKA_PAYLOAD_TYPE:subserver.data}")
    private String kafkaPayloadType;

    @Value("${KAFKA_SOURCE:grpc-kafka-bridge}")
    private String kafkaSource;

    @Value("${KAFKA_VERSION:1.0}")
    private String kafkaVersion;

    // ── Getters ─────────────────────────────────────────────────────────────

    public String getGrpcHost() { return grpcHost; }
    public int getGrpcPort() { return grpcPort; }
    public long getGrpcIotId() { return grpcIotId; }
    public int getGrpcReconnectDelayMs() { return grpcReconnectDelayMs; }

    public String getKafkaTopic() { return kafkaTopic; }
    public String getKafkaPayloadType() { return kafkaPayloadType; }
    public String getKafkaSource() { return kafkaSource; }
    public String getKafkaVersion() { return kafkaVersion; }
}
