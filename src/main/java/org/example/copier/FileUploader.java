package org.example.copier;

import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class FileUploader {
    private static final Logger logger = Logger.getLogger(FileUploader.class.getName());

    public static void run(Config config) {
        final var acceptedPath = Paths.get(config.acceptedDir());
        final var uploadedPath = Paths.get(config.uploadedDir());
        final var droppedPath = Paths.get(config.droppedDir());
        final var retryLimit = config.retryLimit();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Files.list(acceptedPath)
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        int attempts = 0;
                        boolean uploaded = false;
                        while (attempts < retryLimit && !uploaded && !Thread.currentThread().isInterrupted()) {
                            try {
                                // Wrap the upload in a virtual thread.
                                Thread.startVirtualThread(() -> uploadToGcs(file, config.gcpKeyFile()))
                                      .join();
                                Files.move(file, uploadedPath.resolve(file.getFileName()), StandardCopyOption.ATOMIC_MOVE);
                                logger.info("Uploaded and moved file " + file.getFileName());
                                uploaded = true;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            } catch (Exception e) {
                                attempts++;
                                logger.warning("Upload attempt " + attempts + " failed for " + file.getFileName() + ": " + e.getMessage());
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
                                Files.move(file, droppedPath.resolve(file.getFileName()), StandardCopyOption.ATOMIC_MOVE);
                                logger.info("Moved file " + file.getFileName() + " to dropped after " + attempts + " attempts");
                            } catch (Exception e) {
                                logger.severe("Failed to move dropped file " + file.getFileName() + ": " + e.getMessage());
                            }
                        }
                    });
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                logger.info("FileUploader interrupted, stopping.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warning("Error in FileUploader: " + e.getMessage());
            }
        }
    }

    // Dummy implementation simulating a GCP upload using a key file.
    private static void uploadToGcs(Path file, String gcpKeyFile) {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Upload interrupted");
        }
        logger.info("Simulated upload of " + file.getFileName() + " using key file " + gcpKeyFile);
    }
}
