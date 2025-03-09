package org.example.copier;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.List;
import java.util.logging.Logger;

public class FileFilteringComponent implements Component {
    private static final Logger logger = Logger.getLogger(FileFilteringComponent.class.getName());

    @Override
    public void run(AppConfig config) {
        final var landedPath = Paths.get(config.landedDir());
        final var acceptedPath = Paths.get(config.acceptedDir());
        final var rejectedPath = Paths.get(config.rejectedDir());
        final var failedPath = Paths.get(config.failedDir());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                try (final var stream = Files.list(landedPath)) {
                    final var files = stream.filter(Files::isRegularFile).toList();
                    for (final var file : files) {
                        final var result = analyzeFile(file);
                        final var targetPath = result != null 
                                ? (result ? acceptedPath : rejectedPath)
                                : failedPath;
                        final var target = targetPath.resolve(file.getFileName());
                        Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                        logger.info("Moved file " + file + " to " + targetPath);
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.severe("Error in FileFilteringComponent: " + e.getMessage());
            }
        }
    }

    private Boolean analyzeFile(Path file) {
        try (final var channel = FileChannel.open(file, StandardOpenOption.READ)) {
            final var size = Files.size(file);
            final ByteBuffer mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            if (mappedBuffer.hasRemaining()) {
                final var b = mappedBuffer.get();
                // Dummy predicate: return true if first byte is even, false if odd.
                return (b % 2 == 0);
            }
        } catch (Exception e) {
            logger.warning("Failed to analyze file " + file + ": " + e.getMessage());
        }
        return null;
    }
}
