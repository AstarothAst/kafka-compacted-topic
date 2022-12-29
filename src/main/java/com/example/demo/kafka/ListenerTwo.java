package com.example.demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Service;

@Service
@KafkaListener(topics = {"topic-2"})
@Slf4j
public class ListenerTwo implements ConsumerSeekAware {

}
