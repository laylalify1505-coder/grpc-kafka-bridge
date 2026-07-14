package com.bridge.grpc;

import com.bridge.config.BridgeConfig;
import com.bridge.kafka.KafkaBridgeProducer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import subserver.dataplane.v1.DataEnvelope;
import subserver.dataplane.v1.DataPlaneGrpc;
import subserver.dataplane.v1.SubscribeRequest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumes the gRPC server-streamed DataEnvelopes from the Sub-Server
 * and forwards each envelope to a Kafka topic.
 */
@Service
public class DataPlaneClient {

    private static final Logger log = LoggerFactory.getLogger(DataPlaneClient.class);

    private final BridgeConfig config;
    private final KafkaBridgeProducer producer;

    private ManagedChannel channel;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DataPlaneClient(BridgeConfig config, KafkaBridgeProducer producer) {
        this.config = config;
        this.producer = producer;
    }

    @PostConstruct
    public void start() {
        running.set(true);
        connectAndSubscribe();
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (channel != null && !channel.isShutdown()) {
            channel.shutdownNow();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Opens a gRPC connection and blocks on the server stream.
     * On disconnect, reconnects after a configurable delay.
     */
    private void connectAndSubscribe() {
        while (running.get()) {
            try {
                log.info("Connecting to Sub-Server gRPC at {}:{} ...",
                        config.getGrpc().getHost(), config.getGrpc().getPort());

                channel = ManagedChannelBuilder.forAddress(
                                config.getGrpc().getHost(),
                                config.getGrpc().getPort())
                        .usePlaintext()
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .build();

                var stub = DataPlaneGrpc.newStub(channel);

                var request = SubscribeRequest.newBuilder()
                        .setIotId(config.getGrpc().getIotId())
                        .build();

                var stream = new StreamObserver<DataEnvelope>() {
                    @Override
                    public void onNext(DataEnvelope envelope) {
                        handleEnvelope(envelope);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("gRPC stream error: {}", t.getMessage());
                        // Triggers reconnect in the outer loop
                    }

                    @Override
                    public void onCompleted() {
                        log.info("gRPC stream completed (server closed)");
                    }
                };

                CountDownLatch latch = new CountDownLatch(1);
                    var observingStream = new StreamObserver<DataEnvelope>() {
                        @Override
                        public void onNext(DataEnvelope envelope) {
                            handleEnvelope(envelope);
                        }
                        @Override
                        public void onError(Throwable t) {
                            log.error("gRPC stream error: {}", t.getMessage());
                            latch.countDown();
                        }
                        @Override
                        public void onCompleted() {
                            log.info("gRPC stream completed (server closed)");
                            latch.countDown();
                        }
                    };

                    stub.subscribe(request, observingStream);
                    latch.await();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("gRPC connection failed: {}", e.getMessage());
            }

            if (running.get()) {
                log.info("Reconnecting in {} ms ...", config.getGrpc().getReconnectDelayMs());
                try {
                    Thread.sleep(config.getGrpc().getReconnectDelayMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void handleEnvelope(DataEnvelope envelope) {
        log.debug("Received envelope: iot_id={}, data_type={}",
                envelope.getIotId(), envelope.getDataType());

        producer.sendToKafka(envelope);

        log.info("Forwarded iot_id={} type={} to Kafka topic [{}]",
                envelope.getIotId(), envelope.getDataType(),
                config.getKafka().getTopic());
    }
}
