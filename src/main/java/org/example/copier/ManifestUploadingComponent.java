package org.example.copier;
// File: ManifestUploadingComponent.java
import java.nio.file.*;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

public class ManifestUploadingComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(ManifestUploadingComponent.class.getName());
    
    private final Path manifestsLandedDir = Paths.get("manifests/landed");
    private final Path manifestsIncomingDir = Paths.get("manifests/incoming");
    private final Path manifestsDroppedDir = Paths.get("manifests/dropped");
    private final Path manifestsUploadedDir = Paths.get("manifests/uploaded");
    
    // These parameters could be loaded from configuration.
    private final String gcsBucketName = "my-gcs-bucket";
    private final String gcpKeyFile = "path/to/gcp-key.json";
    private final int retryLimit = 3;
    
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (final var files = Files.list(manifestsLandedDir)) {
                files.filter(Files::isRegularFile).forEach(file -> {
                    int attempts = 0;
                    boolean success = false;
                    while (attempts < retryLimit && !success && !Thread.currentThread().isInterrupted()) {
                        try {
                            // Simulate uploading the manifest file.
                            success = Util.uploadToGCS(file, gcsBucketName, manifestsIncomingDir.getFileName().toString(), gcpKeyFile);
                            if (!success) {
                                attempts++;
                                logger.warning("Manifest upload failed for " + file + ", attempt " + attempts);
                                TimeUnit.SECONDS.sleep(2);
                            }
                        } catch (InterruptedException e) {
                            logger.info("Manifest upload interrupted for " + file);
                            Thread.currentThread().interrupt();
                        }
                    }
                    try {
                        if (success) {
                            Util.atomicMove(file, manifestsUploadedDir.resolve(file.getFileName()));
                        } else {
                            Util.atomicMove(file, manifestsDroppedDir.resolve(file.getFileName()));
                        }
                    } catch (IOException e) {
                        logger.warning("Error moving manifest file " + file + ": " + e.getMessage());
                    }
                });
            } catch (IOException e) {
                logger.warning("Error listing manifests in " + manifestsLandedDir + ": " + e.getMessage());
            }
            try {
                Thread.sleep(5000); // Poll every 5 seconds.
            } catch (InterruptedException e) {
                logger.info("ManifestUploadingComponent interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }
}
