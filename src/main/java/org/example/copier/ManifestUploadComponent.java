package org.example.copier;

// ManifestUploadComponent.java

import java.nio.file.*;
import java.io.IOException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ManifestUploadComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(ManifestUploadComponent.class.getName());
    private final Configuration configuration;

    public ManifestUploadComponent(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run() {
        final var manifestLandedPath = Paths.get(configuration.manifestLanded()).toAbsolutePath();
        final var manifestDroppedPath = Paths.get(configuration.manifestDropped()).toAbsolutePath();
        final var manifestUploadedPath = Paths.get(configuration.manifestUploaded()).toAbsolutePath();
        final var manifestFailedPath = Paths.get(configuration.manifestFailed()).toAbsolutePath();
        final var manifestGCSPath = Paths.get(configuration.gcs()).toAbsolutePath();
        final var manifestIncomingPath = Paths.get(configuration.manifestIncoming()).toAbsolutePath();

        try {
            TimeUnit.SECONDS.sleep(10);

            while (!Thread.currentThread().isInterrupted()) {
                try (final var scope = new StructuredTaskScope.ShutdownOnFailure();
                        final var walk = Files.walk(manifestLandedPath, Integer.MAX_VALUE)) {
                    final var old = Instant.now().minus(1, ChronoUnit.HOURS);
                    walk.filter(Files::isRegularFile)
                            .forEach(manifestPath -> {
                                scope.fork(() -> {
                                    try {
                                        final var relativeManifestPath = manifestLandedPath.relativize(manifestPath);
                                        if (FileUtils.isOlderThan(manifestPath, old)) {
                                            FileUtils.moveFileAtomically(manifestPath,
                                                    manifestDroppedPath.resolve(relativeManifestPath));
                                            logger.info("Manifest " + manifestPath
                                                    + " is older than one hour. Moved to dropped.");
                                            return null;
                                        }

                                        final var lines = Files.readAllLines(manifestPath);
                                        final var uploaded = lines.stream()
                                                .filter(line -> line.contains("uploaded"))
                                                .collect(Collectors.joining(System.lineSeparator()));

                                        // skip upload to gcs and create manifest directly in uploaded folder
                                        final var gcsManifestPath = manifestGCSPath.resolve(relativeManifestPath);
                                        Files.createDirectories(gcsManifestPath.getParent());
                                        Files.writeString(gcsManifestPath, uploaded);

                                        FileUtils.moveFileAtomically(manifestPath,
                                                manifestUploadedPath.resolve(relativeManifestPath));
                                        logger.info(
                                                "Manifest " + manifestPath + " uploaded and moved to uploaded folder.");
                                    } catch (Exception e) {
                                        try {
                                            final var relativeManifestPath = manifestLandedPath
                                                    .relativize(manifestPath);

                                            FileUtils.moveFileAtomically(manifestPath,
                                                    manifestFailedPath.resolve(relativeManifestPath));
                                            logger.severe("Error processing manifest " + manifestPath + ": "
                                                    + e.getMessage());
                                        } catch (IOException ioException) {
                                            logger.severe("Failed to move manifest " + manifestPath
                                                    + " to failed folder: " + ioException.getMessage());
                                        }
                                    }

                                    return null;
                                });
                            });

                    scope.join();
                } catch (IOException e) {
                    logger.severe("Error walking manifest directory: " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("ManifestUploadComponent interrupted.");
        } finally {
            logger.warning("ManifestUploadComponent stopped.");
        }
    }
}
