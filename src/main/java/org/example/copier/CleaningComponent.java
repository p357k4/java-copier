package org.example.copier;
// CleaningComponent.java
import java.nio.file.*;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.stream.Collectors;
import java.util.logging.Logger;

public class CleaningComponent implements Callable<FileStats> {
    private final Path manifestRegisteredDir;
    private final Path manifestCompletedDir;
    private static final Logger logger = Logger.getLogger(CleaningComponent.class.getName());
    
    public CleaningComponent(final String manifestRegisteredDir, final String manifestCompletedDir) {
        this.manifestRegisteredDir = Path.of(manifestRegisteredDir);
        this.manifestCompletedDir = Path.of(manifestCompletedDir);
    }
    
    @Override
    public FileStats call() {
        int processed = 0;
        int failed = 0;
        try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            final var files = Files.walk(manifestRegisteredDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableList());
            for (final var file : files) {
                scope.fork(() -> {
                    try {
                        final var target = manifestCompletedDir.resolve(file.getFileName());
                        Files.createDirectories(target.getParent());
                        Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                        return 1;
                    } catch (final Exception e) {
                        logger.severe("Failed to clean manifest " + file + ": " + e.getMessage());
                        throw e;
                    }
                });
                processed++;
            }
            scope.join();
            scope.throwIfFailed();
        } catch (final Exception e) {
            logger.severe("Error in CleaningComponent: " + e.getMessage());
            failed++;
        }
        return new FileStats(processed, failed);
    }
}
