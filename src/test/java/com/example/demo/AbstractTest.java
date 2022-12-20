package com.example.demo;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("unchecked")
public abstract class AbstractTest {

    @BeforeEach
    public void beforeEach() {
        MockitoAnnotations.openMocks(this);
    }

    public void assertMessageContains(Throwable e, String... patterns) {
        if (!isMessageContains(e, patterns)) {
            throw new RuntimeException("Patterns not found: " + e.getMessage());
        }
    }

    public int randomInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    public Duration randomDuration() {
        return Duration.ofSeconds(randomInt());
    }

    private boolean isMessageContains(Throwable e, String... patterns) {
        String message = e.getMessage();
        return Arrays.stream(patterns).allMatch(message::contains);
    }
}
