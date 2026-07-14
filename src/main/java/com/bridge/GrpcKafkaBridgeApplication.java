package com.bridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GrpcKafkaBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(GrpcKafkaBridgeApplication.class, args);
    }
}
