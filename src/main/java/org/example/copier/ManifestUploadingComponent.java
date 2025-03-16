package org.example.copier;

// ManifestUploadingComponent.java
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Logger;

public class ManifestUploadingComponent implements Callable<FileStats> {
    private final Path manifestLandedDir;
    private final Path manifestDroppedDir;
    private final Path manifestUploadedDir;
    private final Path manifestFailedDir;
    private final Path gcpDir;
    private static final Logger logger = Logger.getLogger(ManifestUploadingComponent.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public record FileInfo(String path, long size) {}
    public record ManifestRecord(List<FileInfo> files, String createdAt) {}

    public ManifestUploadingComponent(final String manifestLandedDir, final String manifestDroppedDir,
                                      final String manifestUploadedDir, final String manifestFailedDir, final String gcpDir) {
        this.manifestLandedDir = Path.of(manifestLandedDir);
        this.manifestDroppedDir = Path.of(manifestDroppedDir);
        this.manifestUploadedDir = Path.of(manifestUploadedDir);
        this.manifestFailedDir = Path.of(manifestFailedDir);
        this.gcpDir = Path.of(gcpDir);
    }
    
    @Override
    public FileStats call() {
        int processed = 0;
        int failed = 0;
        try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            final var manifestFiles = Files.walk(manifestLandedDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableList());
            for (final var manifest : manifestFiles) {
                scope.fork(() -> {
                    try {
                        final var lastModified = Files.getLastModifiedTime(manifest).toInstant();
                        if (Duration.between(lastModified, Instant.now()).toHours() >= 1) {
                            final var target = manifestDroppedDir.resolve(manifest.getFileName());
                            Files.createDirectories(target.getParent());
                            Files.move(manifest, target, StandardCopyOption.ATOMIC_MOVE);
                        } else {
                            final var originalManifest = mapper.readValue(manifest.toFile(), ManifestRecord.class);
                            // Filter: for example, only include files that still exist (simulate filtering for uploaded files)
                            final var filteredFiles = originalManifest.files().stream()
                                    .filter(fileInfo -> Files.exists(Path.of(fileInfo.path())))
                                    .collect(Collectors.toList());
                            final var tempManifest = new ManifestRecord(filteredFiles, Instant.now().toString());
                            // Simulate upload to GCS by writing to a subfolder in the gcp directory
                            final var gcpTarget = gcpDir.resolve("manifests_incoming").resolve(manifest.getFileName());
                            Files.createDirectories(gcpTarget.getParent());
                            mapper.writeValue(gcpTarget.toFile(), tempManifest);
                            final var target = manifestUploadedDir.resolve(manifest.getFileName());
                            Files.createDirectories(target.getParent());
                            Files.move(manifest, target, StandardCopyOption.ATOMIC_MOVE);
                        }
                        return 1;
                    } catch (final Exception e) {
                        logger.severe("Failed to process manifest uploading for " + manifest + ": " + e.getMessage());
                        final var target = manifestFailedDir.resolve(manifest.getFileName());
                        try {
                            Files.createDirectories(target.getParent());
                            Files.move(manifest, target, StandardCopyOption.ATOMIC_MOVE);
                        } catch (final Exception ex) {
                            logger.severe("Failed to move failed manifest " + manifest + ": " + ex.getMessage());
                        }
                        throw e;
                    }
                });
                processed++;
            }
            scope.join();
            scope.throwIfFailed();
        } catch (final Exception e) {
            logger.severe("Error in ManifestUploadingComponent: " + e.getMessage());
            failed++;
        }
        return new FileStats(processed, failed);
    }
}
