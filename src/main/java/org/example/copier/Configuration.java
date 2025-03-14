package org.example.copier;

// Configuration.java
import java.nio.file.Path;
import java.util.List;

public record Configuration(
    Path incomingFolder,
    Path landedFolder,
    Path acceptedFolder,
    Path rejectedFolder,
    Path failedFolder,
    Path uploadedFolder,
    Path droppedFolder,
    Path completedFolder,
    Path manifestsLandedFolder,
    Path manifestsIncomingFolder,
    Path manifestsUploadedFolder,
    Path manifestsFailedFolder,
    Path manifestsDroppedFolder,
    Path manifestsCompletedFolder,
    int retryLimit,
    String gcsBucket,
    String gcsKeyFile,
    int manifestMaxFiles,
    long manifestIntervalMinutes
) {
    public List<Path> getAllDirectories() {
        return List.of(
            incomingFolder,
            landedFolder,
            acceptedFolder,
            rejectedFolder,
            failedFolder,
            uploadedFolder,
            droppedFolder,
            completedFolder,
            manifestsLandedFolder,
            manifestsIncomingFolder,
            manifestsUploadedFolder,
            manifestsFailedFolder,
            manifestsDroppedFolder,
            manifestsCompletedFolder
        );
    }
}
