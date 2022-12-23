package com.example.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.DELETE_RETENTION_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG;

public class TopicConfigBuilder {

    private String cleanupPolicy;
    private String maxCompactionLagMs;
    private String deleteRetentionMs;

    public static TopicConfigBuilder cleanupPolicy(String cleanupPolicy){
        return new TopicConfigBuilder(cleanupPolicy);
    }

    public TopicConfigBuilder(String cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
    }

    public TopicConfigBuilder setMaxCompactionLagMs(Long maxCompactionLagMs) {
        this.maxCompactionLagMs = maxCompactionLagMs.toString();
        return this;
    }

    public TopicConfigBuilder setDeleteRetentionMs(Long deleteRetentionMs) {
        this.deleteRetentionMs = deleteRetentionMs.toString();
        return this;
    }

    public TopicConfigBuilder setCleanupPolicy(String cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
        return this;
    }

    public Map<String, String> build() {
        Map<String, String> config = new HashMap<>();

        Optional.ofNullable(maxCompactionLagMs)
                .ifPresent(value -> config.putIfAbsent(MAX_COMPACTION_LAG_MS_CONFIG, value));
        Optional.ofNullable(deleteRetentionMs)
                .ifPresent(value -> config.putIfAbsent(DELETE_RETENTION_MS_CONFIG, value));
        Optional.ofNullable(cleanupPolicy)
                .ifPresent(value -> config.putIfAbsent(CLEANUP_POLICY_CONFIG, value));

        return config;
    }
}
