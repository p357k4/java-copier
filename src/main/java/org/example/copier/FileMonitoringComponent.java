package org.example.copier;
// File: FileMonitoringComponent.java
import java.nio.file.*;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

public class FileMonitoringComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(FileMonitoringComponent.class.getName());
    private final Path incomingDir = Paths.get("files/incoming");
    private final Path landedDir = Paths.get("files/landed");
    
    @Override
    public void run() {
        final var previousSizes = new HashMap<String, Long>();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final var currentSizes = Files.list(incomingDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toMap(
                        path -> path.getFileName().toString(),
                        path -> {
                            try {
                                return Files.size(path);
                            } catch (IOException e) {
                                logger.warning("Error getting size for " + path + ": " + e.getMessage());
                                return -1L;
                            }
                        }
                    ));
                
                for (final var entry : currentSizes.entrySet()) {
                    final var fileName = entry.getKey();
                    final var currentSize = entry.getValue();
                    final var previousSize = previousSizes.get(fileName);
                    if (previousSize != null && previousSize == currentSize && currentSize != -1L) {
                        // Move file atomically to the landed folder.
                        final var source = incomingDir.resolve(fileName);
                        final var target = landedDir.resolve(fileName);
                        try {
                            Util.atomicMove(source, target);
                        } catch (IOException e) {
                            logger.warning("Failed to move file " + fileName + ": " + e.getMessage());
                        }
                    }
                }
                // Update previous sizes.
                previousSizes.clear();
                previousSizes.putAll(currentSizes);
                // Sleep for the configured interval (here one minute).
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            } catch (InterruptedException e) {
                logger.info("FileMonitoringComponent interrupted");
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.warning("I/O error in FileMonitoringComponent: " + e.getMessage());
            }
        }
    }
}
