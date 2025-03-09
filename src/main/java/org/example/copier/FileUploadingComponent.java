package org.example.copier;
// File: FileUploadingComponent.java
import java.nio.file.*;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

public class FileUploadingComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(FileUploadingComponent.class.getName());
    private final Path acceptedDir = Paths.get("files/accepted");
    private final Path uploadedDir = Paths.get("files/uploaded");
    private final Path droppedDir = Paths.get("files/dropped");
    private final Path confirmedDir = Paths.get("files/confirmed");
    
    // These parameters could also be loaded from the configuration.
    private final String gcsBucketName = "my-gcs-bucket";
    private final String gcpKeyFile = "path/to/gcp-key.json";
    private final int retryLimit = 3;

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (final var files = Files.list(acceptedDir)) {
                files.filter(Files::isRegularFile).forEach(file -> {
                    int attempts = 0;
                    boolean success = false;
                    while (attempts < retryLimit && !success && !Thread.currentThread().isInterrupted()) {
                        try {
                            // Simulate upload.
                            success = Util.uploadToGCS(file, gcsBucketName, uploadedDir.getFileName().toString(), gcpKeyFile);
                            if (!success) {
                                attempts++;
                                logger.warning("Upload failed for " + file + ", attempt " + attempts);
                                TimeUnit.SECONDS.sleep(2);
                            }
                        } catch (InterruptedException e) {
                            logger.info("Upload interrupted for " + file);
                            Thread.currentThread().interrupt();
                        }
                    }
                    try {
                        if (success) {
                            // Create a confirmation record and write it as JSON.
                            final var confirmation = new Confirmation(file.toString(), "/gcs/" + file.getFileName());
                            final var confirmationFile = confirmedDir.resolve(file.getFileName().toString() + ".json");
                            Util.writeJson(confirmationFile, confirmation);
                            // Move the file to the uploaded folder.
                            Util.atomicMove(file, uploadedDir.resolve(file.getFileName()));
                        } else {
                            // Move the file to the dropped folder.
                            Util.atomicMove(file, droppedDir.resolve(file.getFileName()));
                        }
                    } catch (IOException e) {
                        logger.warning("Error moving file " + file + ": " + e.getMessage());
                    }
                });
            } catch (IOException e) {
                logger.warning("Error listing files in " + acceptedDir + ": " + e.getMessage());
            }
            try {
                Thread.sleep(5000); // Poll every 5 seconds.
            } catch (InterruptedException e) {
                logger.info("FileUploadingComponent interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }
}
