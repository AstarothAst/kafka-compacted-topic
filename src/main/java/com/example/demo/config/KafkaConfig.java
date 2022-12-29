package com.example.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

    @Bean
    DefaultErrorHandler globalErrorHandler() {
        return new DefaultErrorHandler((rec, e) -> {
            log.error("Глобальная ошибка в листнере кафки: {}, {}", e.getMessage(), e.getCause().getMessage());
        }, new FixedBackOff(0L, 0L));
    }
}
