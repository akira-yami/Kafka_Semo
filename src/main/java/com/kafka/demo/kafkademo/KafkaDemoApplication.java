package com.kafka.demo.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * ============================================================================
 * CLASS: KafkaDemoApplication
 * VAI TRÒ: Điểm khởi đầu (Entrypoint) của toàn bộ ứng dụng Spring Boot.
 * CÔNG NGHỆ: Spring Boot 4.1.2 & Java 25.
 * 
 * NHIỆM VỤ:
 * - Kích hoạt cơ chế tự động cấu hình (@SpringBootApplication).
 * - Quét các Component, Service, Controller, Configuration trong package.
 * - Khởi chạy Embedded Web Server (Tomcat) trên cổng 8082.
 * ============================================================================
 */
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        // Khởi động ngữ cảnh Spring ApplicationContext và chạy các dịch vụ
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}
