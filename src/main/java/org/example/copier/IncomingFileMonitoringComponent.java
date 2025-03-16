package org.example.copier;
// IncomingFileMonitor.java
// IncomingFileMonitoringComponent.java
import java.nio.file.*;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;

public class IncomingFileMonitoringComponent implements Callable<FileStats> {
    private final Path incomingDir;
    private final Path landedDir;
    private Map<Path, Long> previousFiles = Map.of(); // initially empty
    private static final Logger logger = Logger.getLogger(IncomingFileMonitoringComponent.class.getName());
    
    public IncomingFileMonitoringComponent(final String incomingDir, final String landedDir) {
        this.incomingDir = Path.of(incomingDir);
        this.landedDir = Path.of(landedDir);
    }
    
    @Override
    public FileStats call() {
        int processed = 0;
        int failed = 0;
        try {
            final var currentFiles = Files.walk(incomingDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableMap(
                        path -> path,
                        path -> {
                            try {
                                return Files.size(path);
                            } catch (final Exception e) {
                                logger.severe("Failed to get size for " + path + ": " + e.getMessage());
                                return -1L;
                            }
                        }
                    ));
            
            try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                for (final var entry : currentFiles.entrySet()) {
                    final var path = entry.getKey();
                    final var size = entry.getValue();
                    if (size < 0) continue; // skip errored files
                    
                    final var previousSize = previousFiles.get(path);
                    if (previousSize != null && previousSize.equals(size)) {
                        scope.fork(() -> {
                            try {
                                final var target = landedDir.resolve(incomingDir.relativize(path));
                                Files.createDirectories(target.getParent());
                                Files.move(path, target, StandardCopyOption.ATOMIC_MOVE);
                                return 1;
                            } catch (final Exception e) {
                                logger.severe("Failed to move file " + path + ": " + e.getMessage());
                                throw e;
                            }
                        });
                        processed++;
                    }
                }
                scope.join();
                scope.throwIfFailed();
            }
            previousFiles = currentFiles; // update for next iteration
        } catch (final Exception e) {
            logger.severe("Error in IncomingFileMonitoringComponent: " + e.getMessage());
            failed++;
        }
        return new FileStats(processed, failed);
    }
}
