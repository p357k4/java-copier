package org.example.copier;

// FileFilterComponent.java

import java.nio.file.*;
import java.io.IOException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class FileFilterComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(FileFilterComponent.class.getName());
    private final Configuration configuration;

    public FileFilterComponent(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run() {
        final var landedPath = Paths.get(configuration.landed()).toAbsolutePath();
        final var acceptedPath = Paths.get(configuration.accepted()).toAbsolutePath();
        final var rejectedPath = Paths.get(configuration.rejected()).toAbsolutePath();
        final var failedPath = Paths.get(configuration.failed()).toAbsolutePath();

        try {
            TimeUnit.SECONDS.sleep(10);
            while (!Thread.currentThread().isInterrupted()) {
                try (final var scope = new StructuredTaskScope.ShutdownOnFailure();
                        final var walk = Files.walk(landedPath, Integer.MAX_VALUE)) {
                    walk.filter(Files::isRegularFile)
                            .forEach(filePath -> {
                                final var relativeFilePath = landedPath.relativize(filePath);
                                scope.fork(() -> {
                                    try {
                                        final var predicateResult = analyzeFile(filePath);
                                        // Demonstrate a switch expression (with pattern matching on Boolean)
                                        final Path targetDir = predicateResult ? acceptedPath : rejectedPath;

                                        final var targetPath = targetDir.resolve(relativeFilePath);

                                        FileUtils.moveFileAtomically(filePath, targetPath);
                                        logger.info("File " + filePath + " moved to " + targetPath);
                                    } catch (Exception e) {
                                        try {
                                            final var targetPath = failedPath.resolve(relativeFilePath);

                                            FileUtils.moveFileAtomically(filePath, targetPath);
                                            logger.severe("Error processing file " + filePath + ": " + e.getMessage());
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
                    logger.severe("Error walking directory " + landedPath + ": " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("FileFilterComponent interrupted.");
        } finally {
            logger.warning("FileFilterComponent stopped.");
        }
    }

    private boolean analyzeFile(Path file) throws IOException {
        return true;
        // try (final var channel = FileChannel.open(file, StandardOpenOption.READ)) {
        // final var fileSize = channel.size();
        // return fileSize % 2 == 0;
        // }
    }
}
