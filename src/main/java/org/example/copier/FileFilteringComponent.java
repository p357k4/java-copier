package org.example.copier;
// File: FileFilteringComponent.java
import java.nio.file.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

public class FileFilteringComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(FileFilteringComponent.class.getName());
    private final Path landedDir = Paths.get("files/landed");
    private final Path acceptedDir = Paths.get("files/accepted");
    private final Path rejectedDir = Paths.get("files/rejected");
    private final Path failedDir = Paths.get("files/failed");

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (final var files = Files.list(landedDir)) {
                files.filter(Files::isRegularFile).forEach(file -> {
                    try {
                        final var result = analyzeFile(file);
                        // Use a switch expression to determine the target folder.
                        final var targetDir = switch (result) {
                            case TRUE -> acceptedDir;
                            case FALSE -> rejectedDir;
                            default -> failedDir;
                        };
                        final var target = targetDir.resolve(file.getFileName());
                        Util.atomicMove(file, target);
                    } catch (IOException e) {
                        logger.warning("Error processing file " + file + ": " + e.getMessage());
                    }
                });
            } catch (IOException e) {
                logger.warning("Error listing files in " + landedDir + ": " + e.getMessage());
            }
            try {
                Thread.sleep(5000); // Poll every 5 seconds.
            } catch (InterruptedException e) {
                logger.info("FileFilteringComponent interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private enum FilterResult { TRUE, FALSE, UNKNOWN }
    
    private FilterResult analyzeFile(Path file) {
        // Use a memory-mapped file for content analysis.
        try (final var channel = FileChannel.open(file, StandardOpenOption.READ)) {
            final var size = channel.size();
            final var buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            // Dummy predicate: if the first byte is even then TRUE, if odd then FALSE.
            if (size > 0) {
                final var firstByte = buffer.get(0);
                return (firstByte % 2 == 0) ? FilterResult.TRUE : FilterResult.FALSE;
            }
        } catch (IOException e) {
            logger.warning("Failed to analyze file " + file + ": " + e.getMessage());
        }
        return FilterResult.UNKNOWN;
    }
}
