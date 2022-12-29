package com.example.demo.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.metrics.MetricsEndpoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MetricsController {

    private final MetricsEndpoint metricsEndpoint;

    private record Stat(String name, List<Double> values) {}

    @GetMapping("/all")
    public List<Stat> getAllMetrics() {
        return metricsEndpoint.listNames().getNames().stream()
                .filter(name -> name.contains("tomcat"))
                .map(name -> {
                    MetricsEndpoint.MetricDescriptor metric = metricsEndpoint.metric(name, Collections.emptyList());
                    List<Double> values = metric.getMeasurements().stream().map(MetricsEndpoint.Sample::getValue).toList();
                    return new Stat(name, values);
                })
                .toList();
    }
}
