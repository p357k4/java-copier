package org.example.copier;

// A record to hold configuration values deserialized from JSON.
public record Config(
    String gcpKeyFile,
    int retryLimit,
    String incomingDir,
    String landedDir,
    String acceptedDir,
    String rejectedDir,
    String failedDir,
    String droppedDir,
    String uploadedDir,
    String completedUploadedDir,
    String completedRejectedDir,
    String completedDroppedDir,
    String completedFailedDir,
    String manifestsUploadedDir,
    String manifestsRejectedDir,
    String manifestsDroppedDir,
    String manifestsFailedDir,
    String manifestsLandedDir,
    String manifestsIncomingDir,
    String manifestsCompletedUploadedDir,
    String manifestsCompletedRejectedDir,
    String manifestsCompletedDroppedDir,
    String manifestsCompletedFailedDir,
    String manifestsCompletedUberDir
) {}
