package com.example.demo.controller;

import com.example.demo.dto.KafkaMessageDto;
import com.example.demo.dto.UnknownDto;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/send")
    public String sendMessage(@RequestBody(required = false) Integer topicNum) {
        KafkaMessageDto data = KafkaMessageDto.builder()
                .value("some-string")
                .build();
        return send(topicNum, data);
    }

    @PostMapping("/send-unknown")
    public String sendUnknownMessage(@RequestBody(required = false) Integer topicNum) {
        return send(topicNum, UnknownDto.empty());
    }

    private String send(Integer topicNum, Object data) {
        String key = UUID.randomUUID().toString();
        kafkaTemplate.send(getTopicName(topicNum), key, data);
        return key;
    }

    private String send(Integer topicNum, String key, Object data) {
        kafkaTemplate.send(getTopicName(topicNum), key, data);
        return key;
    }

    private String getTopicName(Integer topicNum) {
        return Optional.ofNullable(topicNum)
                .map(this::computeTopicName)
                .orElse(computeTopicName(1));
    }

    private String computeTopicName(Integer num) {
        return format("topic-%s", num);
    }
}
