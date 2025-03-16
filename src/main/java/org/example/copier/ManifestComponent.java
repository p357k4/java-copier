package org.example.copier;

// ManifestComponent.java
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Logger;

public class ManifestComponent implements Callable<FileStats> {
    private final Path uploadedDir;
    private final Path rejectedDir;
    private final Path droppedDir;
    private final Path failedDir;
    private final Path completedDir;
    private final Path manifestIncomingDir;
    private final Path manifestLandedDir;
    private static final Logger logger = Logger.getLogger(ManifestComponent.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    // Records for manifest entries
    public record FileInfo(String path, long size) {}
    public record ManifestRecord(List<FileInfo> files, String createdAt) {}
    
    public ManifestComponent(final String uploadedDir, final String rejectedDir, final String droppedDir, final String failedDir,
                             final String completedDir, final String manifestIncomingDir, final String manifestLandedDir) {
        this.uploadedDir = Path.of(uploadedDir);
        this.rejectedDir = Path.of(rejectedDir);
        this.droppedDir = Path.of(droppedDir);
        this.failedDir = Path.of(failedDir);
        this.completedDir = Path.of(completedDir);
        this.manifestIncomingDir = Path.of(manifestIncomingDir);
        this.manifestLandedDir = Path.of(manifestLandedDir);
    }
    
    @Override
    public FileStats call() {
        int processed = 0;
        int failed = 0;
        try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            // Process any existing manifest files from manifestIncomingDir
            final var manifestFiles = Files.walk(manifestIncomingDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableList());
            
            for (final var manifest : manifestFiles) {
                scope.fork(() -> {
                    try {
                        final var manifestRec = mapper.readValue(manifest.toFile(), ManifestRecord.class);
                        for (final var fileInfo : manifestRec.files()) {
                            final var filePath = Path.of(fileInfo.path());
                            final var target = completedDir.resolve(filePath.getFileName());
                            Files.createDirectories(target.getParent());
                            Files.move(filePath, target, StandardCopyOption.ATOMIC_MOVE);
                        }
                        final var targetManifest = manifestLandedDir.resolve(manifest.getFileName());
                        Files.createDirectories(targetManifest.getParent());
                        Files.move(manifest, targetManifest, StandardCopyOption.ATOMIC_MOVE);
                        return 1;
                    } catch (final Exception e) {
                        logger.severe("Failed to process manifest " + manifest + ": " + e.getMessage());
                        throw e;
                    }
                });
                processed++;
            }
            
            // Create new manifest if threshold met (e.g. every one hour or when file count exceeds 10,000)
            final var monitoredDirs = List.of(uploadedDir, rejectedDir, droppedDir, failedDir);
            final List<Path> monitoredFiles = new ArrayList<>();
            for (final var dir : monitoredDirs) {
                monitoredFiles.addAll(Files.walk(dir)
                        .filter(Files::isRegularFile)
                        .collect(Collectors.toList()));
            }
            if (monitoredFiles.size() >= 10000 || Instant.now().getEpochSecond() % 3600 < 60) {
                final List<FileInfo> fileInfos = monitoredFiles.stream()
                        .map(path -> {
                            try {
                                return new FileInfo(path.toString(), Files.size(path));
                            } catch (final Exception e) {
                                logger.severe("Error getting size for " + path + ": " + e.getMessage());
                                return new FileInfo(path.toString(), -1);
                            }
                        })
                        .collect(Collectors.toList());
                final var manifestRecord = new ManifestRecord(fileInfos, Instant.now().toString());
                final var manifestFile = manifestIncomingDir.resolve("manifest_" + Instant.now().toString().replace(":", "-") + ".json");
                Files.createDirectories(manifestFile.getParent());
                mapper.writeValue(manifestFile.toFile(), manifestRecord);
                processed++;
            }
            
            scope.join();
            scope.throwIfFailed();
        } catch (final Exception e) {
            logger.severe("Error in ManifestComponent: " + e.getMessage());
            failed++;
        }
        return new FileStats(processed, failed);
    }
}
