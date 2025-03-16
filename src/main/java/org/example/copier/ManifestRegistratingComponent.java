package org.example.copier;

// ManifestRegistratingComponent.java
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.stream.Collectors;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Logger;

public class ManifestRegistratingComponent implements Callable<FileStats> {
    private final Path manifestUploadedDir;
    private final Path manifestRegisteredDir;
    private final Path manifestFailedDir;
    private static final Logger logger = Logger.getLogger(ManifestRegistratingComponent.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public record FileInfo(String path, long size) {}
    public record ManifestRecord(List<FileInfo> files, String createdAt) {}

    public ManifestRegistratingComponent(final String manifestUploadedDir, final String manifestRegisteredDir, final String manifestFailedDir) {
        this.manifestUploadedDir = Path.of(manifestUploadedDir);
        this.manifestRegisteredDir = Path.of(manifestRegisteredDir);
        this.manifestFailedDir = Path.of(manifestFailedDir);
    }
    
    @Override
    public FileStats call() {
        int processed = 0;
        int failed = 0;
        try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            final var manifestFiles = Files.walk(manifestUploadedDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableList());
            for (final var manifest : manifestFiles) {
                scope.fork(() -> {
                    try {
                        final var manifestRec = mapper.readValue(manifest.toFile(), ManifestRecord.class);
                        // Simulate registration by computing dummy counts (for example, based on file size parity)
                        final var counts = manifestRec.files().stream().collect(Collectors.groupingBy(
                            fileInfo -> (fileInfo.size() % 2 == 0) ? "uploaded" : "failed",
                            Collectors.counting()
                        ));
                        logger.warning("Registering manifest " + manifest.getFileName() + " - " + counts);
                        final var target = manifestRegisteredDir.resolve(manifest.getFileName());
                        Files.createDirectories(target.getParent());
                        Files.move(manifest, target, StandardCopyOption.ATOMIC_MOVE);
                        return 1;
                    } catch (final Exception e) {
                        logger.severe("Failed to register manifest " + manifest + ": " + e.getMessage());
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
            logger.severe("Error in ManifestRegistratingComponent: " + e.getMessage());
            failed++;
        }
        return new FileStats(processed, failed);
    }
}
