package com.example.demo;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class KafkaController {

    public static final String TOPIC_NAME = "compacted-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Admin kafkaClientAdmin;

    private final Map<String, Integer> keyToCounterMap = new HashMap<>();

    @PostMapping("/add")
    public void addRecord(@RequestBody AddDto dto) {
        Integer count = keyToCounterMap.merge(dto.key(), 1, Integer::sum);
        kafkaTemplate.send(TOPIC_NAME, dto.key(), "Count is " + count);
    }

    @PostMapping("/create")
    public void createTopic() {
        Map<String, String> config = new HashMap<>();
        config.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        config.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "20000");
        config.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100");

        
        NewTopic topic = new NewTopic(TOPIC_NAME, 1, (short) 1)
                .configs(config);
        kafkaClientAdmin.createTopics(List.of(topic));
    }

    public record AddDto(String key) {}
}
