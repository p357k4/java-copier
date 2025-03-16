package org.example.copier;

// CleaningComponent.java

import java.nio.file.*;
import java.io.IOException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class CleaningComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(CleaningComponent.class.getName());
    private final Configuration configuration;

    public CleaningComponent(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run() {
        final var manifestUploadedPath = Paths.get(configuration.manifestUploaded()).toAbsolutePath();
        final var manifestCompletedPath = Paths.get(configuration.manifestCompleted()).toAbsolutePath();

        try {
            TimeUnit.SECONDS.sleep(10);
            while (!Thread.currentThread().isInterrupted()) {
                try (final var scope = new StructuredTaskScope.ShutdownOnFailure();
                        final var walk = Files.walk(manifestUploadedPath, Integer.MAX_VALUE)) {
                    walk.filter(Files::isRegularFile)
                            .forEach(manifestPath -> {
                                scope.fork(() -> {
                                    try {
                                        final var relativeManifestPath = manifestUploadedPath.relativize(manifestPath);

                                        final var targetPath = manifestCompletedPath.resolve(relativeManifestPath);
                                        FileUtils.moveFileAtomically(manifestPath, targetPath);
                                        logger.info("Cleaned manifest file " + manifestPath + " moved to completed.");
                                    } catch (IOException e) {
                                        logger.severe("Failed to clean manifest file " + manifestPath + ": "
                                                + e.getMessage());
                                    }

                                    return null;
                                });
                            });

                    scope.join();
                } catch (IOException e) {
                    logger.severe("Error walking manifest uploaded directory: " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("CleaningComponent interrupted.");
        } finally {
            logger.warning("CleaningComponent stopped.");
        }
    }
}
