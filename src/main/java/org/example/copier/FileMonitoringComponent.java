package org.example.copier;

import java.nio.file.*;
import java.util.List;
import java.util.logging.Logger;

public class FileMonitoringComponent implements Component {
    private static final Logger logger = Logger.getLogger(FileMonitoringComponent.class.getName());

    @Override
    public void run(AppConfig config) {
        final var incomingPath = Paths.get(config.incomingDir());
        final var landedPath = Paths.get(config.landedDir());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                try (final var stream = Files.list(incomingPath)) {
                    final var files = stream.filter(Files::isRegularFile).toList();
                    for (final var file : files) {
                        final var size1 = Files.size(file);
                        Thread.sleep(1000); // wait 1 second to check stability
                        final var size2 = Files.size(file);
                        if (size1 == size2) {
                            final var target = landedPath.resolve(file.getFileName());
                            Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            logger.info("Moved file to landed: " + file);
                        }
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.severe("Error in FileMonitoringComponent: " + e.getMessage());
            }
        }
    }
}