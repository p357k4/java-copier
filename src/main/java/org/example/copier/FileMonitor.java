package org.example.copier;

import java.nio.file.*;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

public class FileMonitor {
    private static final Logger logger = Logger.getLogger(FileMonitor.class.getName());

    public static void run(Config config) {
        final var incomingPath = Paths.get(config.incomingDir());
        final var landedPath = Paths.get(config.landedDir());
        Map<String, Long> previousSizes = new HashMap<>();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // List files and get their sizes wrapped in a virtual thread if needed.
                final var currentSizes = Files.list(incomingPath)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toMap(
                        path -> path.getFileName().toString(),
                        path -> {
                            try {
                                return Files.size(path);
                            } catch (Exception e) {
                                logger.warning("Failed to get size for " + path + ": " + e.getMessage());
                                return -1L;
                            }
                        }
                    ));

                // Compare current and previous sizes; if a file’s size is unchanged, move it.
                for (final var entry : currentSizes.entrySet()) {
                    final var fileName = entry.getKey();
                    final var size = entry.getValue();
                    if (previousSizes.containsKey(fileName) && previousSizes.get(fileName).equals(size)) {
                        final var source = incomingPath.resolve(fileName);
                        final var target = landedPath.resolve(fileName);
                        try {
                            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
                            logger.info("Moved file " + fileName + " to landed");
                        } catch (Exception e) {
                            logger.warning("Failed to move file " + fileName + ": " + e.getMessage());
                        }
                    }
                }
                previousSizes = currentSizes;
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                logger.info("FileMonitor interrupted, stopping.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warning("Error during monitoring: " + e.getMessage());
            }
        }
    }
}
