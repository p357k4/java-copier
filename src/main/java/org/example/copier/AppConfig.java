package org.example.copier;

import java.util.Objects;

public record AppConfig(
    String gcpKeyFilePath,
    String incomingDir,         // files/incoming
    String landedDir,           // files/landed
    String acceptedDir,         // files/accepted
    String rejectedDir,         // files/rejected
    String failedDir,           // files/failed
    String droppedDir,          // files/dropped
    String uploadedDir,         // files/uploaded (local copy after upload)
    String confirmedDir,        // files/confirmed
    String manifestsConfirmedDir,   // manifests/confirmed
    String manifestsAcceptedDir,    // manifests/accepted
    String manifestsRejectedDir,    // manifests/rejected
    String manifestsDroppedDir,     // manifests/dropped
    String manifestsFailedDir,      // manifests/failed
    String uberManifestDir,         // manifests/landed (uber manifest)
    String manifestsIncomingDir,    // manifests/incoming (upload target)
    String manifestsUploadedDir,    // manifests/uploaded (after upload)
    String completedUploadedDir,    // files/completed/uploaded
    String completedRejectedDir,    // files/completed/rejected
    String completedDroppedDir,     // files/completed/dropped
    String completedFailedDir,      // files/completed/failed
    String manifestsCompletedDir    // manifests/completed
) {
    public AppConfig {
        Objects.requireNonNull(gcpKeyFilePath);
        // (Additional non-null checks as needed)
    }
}