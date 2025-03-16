package org.example.copier;
// Config.java

public record Configuration(
    String incoming,
    String landed,
    String accepted,
    String rejected,
    String failed,
    String dropped,
    String completed,
    String manifestLanded,
    String manifestDropped,
    String manifestUploaded,
    String manifestFailed,
    String manifestIncoming,
    String manifestCompleted,
    String gcs,
    String uploaded
) {}
