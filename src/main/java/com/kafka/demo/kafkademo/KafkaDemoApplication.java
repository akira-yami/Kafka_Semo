package com.kafka.demo.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
        System.out.println("==================================================================");
        System.out.println(" KAFKA & SPRING BOOT DEMO APP STARTED ON PORT 8081");
        System.out.println(" Kafka UI: http://localhost:8080");
        System.out.println(" REST API: http://localhost:8081/api/orders");
        System.out.println("==================================================================");
    }
}
