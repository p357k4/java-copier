package org.example.copier;

import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.io.IOException;

public class ManifestUploader {
    private static final Logger logger = Logger.getLogger(ManifestUploader.class.getName());

    public static void run(Config config) {
        final var manifestsLandedDir = Paths.get(config.manifestsLandedDir());
        final var manifestsIncomingDir = Paths.get(config.manifestsIncomingDir());
        final var manifestsUploadedDir = Paths.get(config.manifestsUploadedDir());
        final var manifestsDroppedDir = Paths.get(config.manifestsDroppedDir());
        final var retryLimit = config.retryLimit();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Files.list(manifestsLandedDir)
                    .filter(Files::isRegularFile)
                    .forEach(uberManifestFile -> {
                        int attempts = 0;
                        boolean uploaded = false;
                        while (attempts < retryLimit && !uploaded && !Thread.currentThread().isInterrupted()) {
                            try {
                                final var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                                final var uberManifest = mapper.readValue(uberManifestFile.toFile(), UberManifest.class);
                                Thread.startVirtualThread(() -> uploadManifest(uberManifest.uploadedManifest(), config.gcpKeyFile()))
                                      .join();
                                Files.move(uberManifestFile, manifestsUploadedDir.resolve(uberManifestFile.getFileName()), StandardCopyOption.ATOMIC_MOVE);
                                logger.info("Uploaded uber manifest " + uberManifestFile.getFileName());
                                uploaded = true;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            } catch (IOException e) {
                                attempts++;
                                logger.warning("Attempt " + attempts + " failed for uber manifest " + uberManifestFile.getFileName() + ": " + e.getMessage());
                                try {
                                    TimeUnit.SECONDS.sleep(5);
                                } catch (InterruptedException ex) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                        }
                        if (!uploaded) {
                            try {
                                Files.move(uberManifestFile, manifestsDroppedDir.resolve(uberManifestFile.getFileName()), StandardCopyOption.ATOMIC_MOVE);
                                logger.info("Moved uber manifest " + uberManifestFile.getFileName() + " to dropped after " + attempts + " attempts");
                            } catch (IOException e) {
                                logger.severe("Failed to move uber manifest " + uberManifestFile.getFileName() + ": " + e.getMessage());
                            }
                        }
                    });
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                logger.info("ManifestUploader interrupted, stopping.");
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.warning("Error in ManifestUploader: " + e.getMessage());
            }
        }
    }

    private static void uploadManifest(Path manifest, String gcpKeyFile) {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Upload interrupted");
        }
        logger.info("Simulated upload of manifest " + manifest.getFileName() + " using key file " + gcpKeyFile);
    }
}
