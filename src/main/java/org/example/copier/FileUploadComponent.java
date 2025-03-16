package org.example.copier;
// FileUploadComponent.java

import java.nio.file.*;
import java.io.IOException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;

public class FileUploadComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(FileUploadComponent.class.getName());
    private final Configuration configuration;

    public FileUploadComponent(final Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run() {
        final var acceptedPath = Paths.get(configuration.accepted()).toAbsolutePath();
        final var droppedPath = Paths.get(configuration.dropped()).toAbsolutePath();
        final var failedPath = Paths.get(configuration.failed()).toAbsolutePath();
        final var uploadedPath = Paths.get(configuration.uploaded()).toAbsolutePath();
        final var gcsPath = Paths.get(configuration.gcs()).toAbsolutePath();

        try {
            TimeUnit.SECONDS.sleep(10);
            
            while (!Thread.currentThread().isInterrupted()) {
                try (final var scope = new StructuredTaskScope.ShutdownOnFailure();
                        final var walk = Files.walk(acceptedPath, Integer.MAX_VALUE)) {
                    final var old = Instant.now().minus(1, ChronoUnit.HOURS);
                    walk.filter(Files::isRegularFile)
                            .forEach(filePath -> {
                                scope.fork(() -> {
                                    try {
                                        final var relativeFilePath = acceptedPath.relativize(filePath);

                                        if (FileUtils.isOlderThan(filePath, old)) {
                                            FileUtils.moveFileAtomically(filePath,
                                                    droppedPath.resolve(relativeFilePath));
                                            logger.info(
                                                    "File " + filePath + " is older than one hour. Moved to dropped.");
                                            return null;
                                        }

                                        // Emulate upload: copy file to GCS folder and then move file to uploaded
                                        // folder.
                                        FileUtils.copyFile(filePath, gcsPath.resolve(relativeFilePath));
                                        final var targetPath = uploadedPath.resolve(relativeFilePath);
                                        FileUtils.moveFileAtomically(filePath, targetPath);
                                        logger.info("File " + filePath + " moved to " + targetPath);
                                    } catch (Exception e) {
                                        try {
                                            final var relativeFilePath = acceptedPath.relativize(filePath);

                                            FileUtils.moveFileAtomically(filePath,
                                                    failedPath.resolve(relativeFilePath));
                                            logger.severe("Error uploading file " + filePath + ": " + e.getMessage());
                                        } catch (IOException ioException) {
                                            logger.severe("Failed to move file " + filePath + " to failed folder: "
                                                    + ioException.getMessage());
                                        }
                                    }

                                    return null;
                                });
                            });

                    scope.join();
                } catch (IOException e) {
                    logger.severe("Error walking directory " + acceptedPath + ": " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("FileUploadComponent interrupted.");
        } finally {
            logger.warning("FileUploadComponent stopped.");
        }
    }
}
