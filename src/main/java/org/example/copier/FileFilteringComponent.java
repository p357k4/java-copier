package org.example.copier;
// FileFilteringComponent.java
import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class FileFilteringComponent implements Callable<FileStats> {
    private final Path landedDir;
    private final Path acceptedDir;
    private final Path rejectedDir;
    private final Path failedDir;
    private static final Logger logger = Logger.getLogger(FileFilteringComponent.class.getName());
    
    public FileFilteringComponent(final String landedDir, final String acceptedDir, final String rejectedDir, final String failedDir) {
        this.landedDir = Path.of(landedDir);
        this.acceptedDir = Path.of(acceptedDir);
        this.rejectedDir = Path.of(rejectedDir);
        this.failedDir = Path.of(failedDir);
    }
    
    private boolean predicate(final MappedByteBuffer buffer) {
        // Dummy predicate: file accepted if first byte is even
        return buffer.get(0) % 2 == 0;
    }
    
    @Override
    public FileStats call() {
        int processed = 0;
        int failed = 0;
        try {
            final var files = Files.walk(landedDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableList());
            try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                for (final var file : files) {
                    scope.fork(() -> {
                        try {
                            final var buffer = mapFile(file);
                            // Use a switch expression with pattern matching style (here switching on a computed string)
                            final var targetDir = switch (predicate(buffer) ? "accepted" : "rejected") {
                                case "accepted" -> acceptedDir;
                                case "rejected" -> rejectedDir;
                                default -> failedDir;
                            };
                            final var target = targetDir.resolve(landedDir.relativize(file));
                            Files.createDirectories(target.getParent());
                            Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            return 1;
                        } catch (final Exception e) {
                            logger.severe("Failed to process file " + file + ": " + e.getMessage());
                            throw e;
                        }
                    });
                    processed++;
                }
                scope.join();
                scope.throwIfFailed();
            }
        } catch (final Exception e) {
            logger.severe("Error in FileFilteringComponent: " + e.getMessage());
            failed++;
        }
        return new FileStats(processed, failed);
    }
    
    private MappedByteBuffer mapFile(final Path file) throws Exception {
        try (final var channel = FileChannel.open(file, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(file));
        }
    }
}
