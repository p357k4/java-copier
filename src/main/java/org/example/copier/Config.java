package org.example.copier;

// File: Config.java
public record Config(
    String gcpKeyFile,
    int retryLimit,
    int fileMonitoringIntervalSeconds,
    int manifestIntervalSeconds,
    int manifestFileCountLimit,
    String gcsBucketName
) {}
