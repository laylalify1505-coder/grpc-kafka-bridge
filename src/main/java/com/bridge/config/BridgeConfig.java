package com.bridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bridge")
public class BridgeConfig {

    private Grpc grpc = new Grpc();
    private Kafka kafka = new Kafka();

    public Grpc getGrpc() { return grpc; }
    public void setGrpc(Grpc grpc) { this.grpc = grpc; }

    public Kafka getKafka() { return kafka; }
    public void setKafka(Kafka kafka) { this.kafka = kafka; }

    public static class Grpc {
        private String host = "localhost";
        private int port = 50051;
        private long iotId = 0;
        private int reconnectDelayMs = 5000;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }

        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }

        public long getIotId() { return iotId; }
        public void setIotId(long iotId) { this.iotId = iotId; }

        public int getReconnectDelayMs() { return reconnectDelayMs; }
        public void setReconnectDelayMs(int reconnectDelayMs) { this.reconnectDelayMs = reconnectDelayMs; }
    }

    public static class Kafka {
        private String topic = "subserver-data";
        private String payloadType = "subserver.data";
        private String source = "grpc-kafka-bridge";
        private String version = "1.0";

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }

        public String getPayloadType() { return payloadType; }
        public void setPayloadType(String payloadType) { this.payloadType = payloadType; }

        public String getSource() { return source; }
        public void setSource(String source) { this.source = source; }

        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }
    }
}
