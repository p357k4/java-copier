package org.example.copier;

// Config.java
public record Config(
    String incomingDir,
    String landedDir,
    String acceptedDir,
    String rejectedDir,
    String failedDir,
    String uploadedDir,
    String droppedDir,
    String completedDir,
    String manifestIncomingDir,
    String manifestLandedDir,
    String manifestUploadedDir,
    String manifestDroppedDir,
    String manifestFailedDir,
    String manifestRegisteredDir,
    String manifestCompletedDir,
    String gcpDir,
    long fileMonitorIntervalMillis,
    long componentSleepMillis
) {}
