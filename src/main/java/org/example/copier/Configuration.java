package org.example.copier;

public record Configuration(
    String filesIncomingDir,
    String filesLandedDir,
    String filesCompressedDir,
    String filesCompletedDir,
    String filesAcceptedDir,
    String filesRejectedDir,
    String filesFailedDir,
    String filesDroppedDir,
    String filesUploadedDir,
    String manifestsIncomingDir,
    String manifestsLandedDir,
    String manifestsUploadedDir,
    String manifestsFailedDir,
    String manifestsRegisteredDir,
    String manifestsCompletedDir,
    String manifestsTemporaryDir,
    String gcpDir,
    long monitorIntervalMillis
) {}
