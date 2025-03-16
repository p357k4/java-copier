package org.example.copier;
// IncomingFileMonitor.java

import java.nio.file.*;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Collections;

public class IncomingFileMonitor implements Runnable {
    private static final Logger logger = Logger.getLogger(IncomingFileMonitor.class.getName());
    private final Configuration configuration;

    public IncomingFileMonitor(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run() {
        final var incomingPath = Paths.get(configuration.incoming()).toAbsolutePath();
        final var landedPath = Paths.get(configuration.landed()).toAbsolutePath();
        var previousMap = Collections.<Path, Long>emptyMap();

        try {
            TimeUnit.SECONDS.sleep(10);
            while (!Thread.currentThread().isInterrupted()) {
                try (final var scope = new StructuredTaskScope.ShutdownOnFailure();
                        final var walk = Files.walk(incomingPath, Integer.MAX_VALUE)) {
                    final var currentMap = walk.filter(Files::isRegularFile)
                            .collect(Collectors.toUnmodifiableMap(
                                    Path::toAbsolutePath,
                                    path -> {
                                        try {
                                            return Files.size(path);
                                        } catch (IOException e) {
                                            logger.severe(
                                                    "Error getting size for file " + path + ": " + e.getMessage());
                                            return 0L;
                                        }
                                    }));

                    // Compare current file sizes with previous map.
                    for (final var entry : currentMap.entrySet()) {
                        final var filePath = entry.getKey();
                        final var prevSize = previousMap.get(filePath);

                        if (prevSize == null) {
                            continue;
                        }

                        if (!prevSize.equals(entry.getValue())) {
                            continue;
                        }

                        scope.fork(() -> {
                            try {
                                final var relativeFilePath = incomingPath.relativize(filePath);
                                final var targetPath = landedPath.resolve(relativeFilePath);

                                FileUtils.moveFileAtomically(filePath, targetPath);
                                logger.info("File " + filePath + " moved to " + targetPath);
                            } catch (Exception e) {
                                logger.severe("Failed to move file " + filePath + ": " + e.getMessage());
                            }
                            return null;
                        });
                    }

                    previousMap = currentMap;
                    scope.join();
                } catch (IOException e) {
                    logger.severe("Error walking manifest directory: " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("IncomingFileMonitor interrupted.");
        } finally {
            logger.warning("IncomingFileMonitor stopped.");
        }
    }
}
