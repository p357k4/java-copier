package org.example.copier;

import java.nio.file.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

public class FileFilter {
    private static final Logger logger = Logger.getLogger(FileFilter.class.getName());

    public static void run(Config config) {
        final var landedPath = Paths.get(config.landedDir());
        final var acceptedPath = Paths.get(config.acceptedDir());
        final var rejectedPath = Paths.get(config.rejectedDir());
        final var failedPath = Paths.get(config.failedDir());

        // Dummy predicate: for example, accept if the first byte is even.
        final Predicate<MappedByteBuffer> predicate = buffer -> buffer.get(0) % 2 == 0;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Files.list(landedPath)
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        try (final var channel = FileChannel.open(file, StandardOpenOption.READ)) {
                            final var buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
                            final var result = predicate.test(buffer);
                            final var target = result 
                                    ? acceptedPath.resolve(file.getFileName()) 
                                    : rejectedPath.resolve(file.getFileName());
                            Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            logger.info("Moved file " + file.getFileName() + " to " + (result ? "accepted" : "rejected"));
                        } catch (Exception e) {
                            try {
                                Files.move(file, failedPath.resolve(file.getFileName()), StandardCopyOption.ATOMIC_MOVE);
                                logger.warning("Failed processing file " + file.getFileName() + ", moved to failed: " + e.getMessage());
                            } catch (IOException ioException) {
                                logger.severe("Failed to move file " + file.getFileName() + " to failed: " + ioException.getMessage());
                            }
                        }
                    });
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                logger.info("FileFilter interrupted, stopping.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warning("Error in FileFilter: " + e.getMessage());
            }
        }
    }
}
