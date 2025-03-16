package org.example.copier;
// ManifestComponent.java

import java.nio.file.*;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class ManifestComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(ManifestComponent.class.getName());
    private final Configuration configuration;
    private static final int MANIFEST_THRESHOLD = 1000;
    private static final long MANIFEST_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);

    public ManifestComponent(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run() {
        final var uploadedPath = Paths.get(configuration.uploaded()).toAbsolutePath();
        final var rejectedPath = Paths.get(configuration.rejected()).toAbsolutePath();
        final var droppedPath = Paths.get(configuration.dropped()).toAbsolutePath();
        final var failedPath = Paths.get(configuration.failed()).toAbsolutePath();
        final var completedPath = Paths.get(configuration.completed()).toAbsolutePath();
        final var manifestLandedPath = Paths.get(configuration.manifestLanded()).toAbsolutePath();

        var next = Instant.now();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                TimeUnit.SECONDS.sleep(10);
                try (final var scope = new StructuredTaskScope.ShutdownOnFailure();
                        final var uploaded = Files.walk(uploadedPath, Integer.MAX_VALUE).filter(Files::isRegularFile);
                        final var rejected = Files.walk(rejectedPath, Integer.MAX_VALUE).filter(Files::isRegularFile);
                        final var dropped = Files.walk(droppedPath, Integer.MAX_VALUE).filter(Files::isRegularFile);
                        final var failed = Files.walk(failedPath, Integer.MAX_VALUE).filter(Files::isRegularFile)) {

                    final var files = Stream.of(uploaded, rejected, dropped, failed).flatMap(Function.identity())
                            .toList();
                    final var now = Instant.now();

                    if (files.isEmpty()) {
                        continue;
                    }

                    if (files.size() < MANIFEST_THRESHOLD && now.isBefore(next)) {
                        continue;
                    }

                    next = now.plus(MANIFEST_INTERVAL_MS, ChronoUnit.MILLIS);
                    final var timestamp = DateTimeFormatter.ISO_INSTANT.format(now.atZone(ZoneOffset.UTC));

                    final var manifestContent = new StringBuilder();
                    for (final var file : files) {
                        try {
                            final var size = Files.size(file);
                            manifestContent.append(file.toAbsolutePath());
                            manifestContent.append(System.lineSeparator());
                        } catch (IOException e) {
                            logger.severe("Failed to get size for file " + file + ": " + e.getMessage());
                        }
                    }

                    final var manifestFile = manifestLandedPath.resolve("manifest-" + timestamp + ".txt");
                    try {
                        Files.writeString(manifestFile, manifestContent.toString());
                        logger.info("Created manifest: " + manifestFile);
                    } catch (IOException e) {
                        logger.severe("Failed to create manifest " + manifestFile + ": " + e.getMessage());
                    }

                    for (final var file : files) {
                        scope.fork(() -> {
                            try {
                                final var filePath = file.toAbsolutePath();
                                final var relativeFilePath = completedPath.getParent().relativize(filePath);
                                final var targetPath = completedPath.resolve(relativeFilePath);

                                FileUtils.moveFileAtomically(file, targetPath);
                                logger.info("Moved file " + filePath + " to completed.");
                            } catch (IllegalArgumentException e) {
                                logger.severe("Failed to calculate " + file + " to completed: " + e.getMessage());
                            } catch (IOException e) {
                                logger.severe("Failed to move file " + file + " to completed: " + e.getMessage());
                            }

                            return null;
                        });
                    }

                    scope.join();
                } catch (IOException e) {
                    logger.severe("Error walking directories: " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("ManifestComponent interrupted.");
        } finally {
            logger.warning("ManifestComponent stopped.");
        }
    }
}