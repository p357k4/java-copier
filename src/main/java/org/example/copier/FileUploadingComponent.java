package org.example.copier;
// FileUploadingComponent.java
import java.nio.file.*;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.stream.Collectors;
import java.util.logging.Logger;

public class FileUploadingComponent implements Callable<FileStats> {
    private final Path acceptedDir;
    private final Path uploadedDir;
    private final Path droppedDir;
    private final Path failedDir;
    private final Path gcpDir;
    private static final Logger logger = Logger.getLogger(FileUploadingComponent.class.getName());
    
    public FileUploadingComponent(final String acceptedDir, final String uploadedDir, final String droppedDir, final String failedDir, final String gcpDir) {
        this.acceptedDir = Path.of(acceptedDir);
        this.uploadedDir = Path.of(uploadedDir);
        this.droppedDir = Path.of(droppedDir);
        this.failedDir = Path.of(failedDir);
        this.gcpDir = Path.of(gcpDir);
    }
    
    @Override
    public FileStats call() {
        int processed = 0;
        int failed = 0;
        try {
            final var files = Files.walk(acceptedDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableList());
            try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                for (final var file : files) {
                    scope.fork(() -> {
                        try {
                            final var lastModified = Files.getLastModifiedTime(file).toInstant();
                            final var now = Instant.now();
                            if (Duration.between(lastModified, now).toHours() >= 1) {
                                final var target = droppedDir.resolve(acceptedDir.relativize(file));
                                Files.createDirectories(target.getParent());
                                Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            } else {
                                // Simulate upload by copying file to the GCP folder
                                final var gcpTarget = gcpDir.resolve(acceptedDir.relativize(file));
                                Files.createDirectories(gcpTarget.getParent());
                                Files.copy(file, gcpTarget, StandardCopyOption.REPLACE_EXISTING);
                                final var target = uploadedDir.resolve(acceptedDir.relativize(file));
                                Files.createDirectories(target.getParent());
                                Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            }
                            return 1;
                        } catch (final Exception e) {
                            logger.severe("Failed to upload file " + file + ": " + e.getMessage());
                            final var target = failedDir.resolve(acceptedDir.relativize(file));
                            try {
                                Files.createDirectories(target.getParent());
                                Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            } catch (final Exception ex) {
                                logger.severe("Failed to move failed file " + file + ": " + ex.getMessage());
                            }
                            throw e;
                        }
                    });
                    processed++;
                }
                scope.join();
                scope.throwIfFailed();
            }
        } catch (final Exception e) {
            logger.severe("Error in FileUploadingComponent: " + e.getMessage());
            failed++;
        }
        return new FileStats(processed, failed);
    }
}
