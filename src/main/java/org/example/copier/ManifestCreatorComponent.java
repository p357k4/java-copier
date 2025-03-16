package org.example.copier;

import java.nio.file.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.StructuredTaskScope;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ManifestCreatorComponent implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(ManifestCreatorComponent.class.getName());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss_SSSSSS").withZone(ZoneOffset.UTC);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Path completedDir;
    private final Path uploadedDir;
    private final Path rejectedDir;
    private final Path failedDir;
    private final Path droppedDir;
    private final Path manifestsLanded;
    private final Path manifestsIncoming;
    private Instant next = Instant.now();
    private final long monitorIntervalMillis;

    public ManifestCreatorComponent(Configuration config) {
        this.manifestsIncoming = Paths.get(config.manifestsIncomingDir());
        this.manifestsLanded = Paths.get(config.manifestsLandedDir());
        this.completedDir = Paths.get(config.filesCompletedDir());
        this.uploadedDir = Paths.get(config.filesUploadedDir());
        this.rejectedDir = Paths.get(config.filesRejectedDir());
        this.failedDir = Paths.get(config.filesFailedDir());
        this.droppedDir = Paths.get(config.filesDroppedDir());
        this.monitorIntervalMillis = config.monitorIntervalMillis();
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        // Process existing manifest files in manifestsIncoming
        try (final var paths = Files.walk(manifestsIncoming).filter(Files::isRegularFile)) {
            paths.forEach(path -> {
                try {
                    final var manifest = objectMapper.readValue(path.toFile(), Manifest.class);
                    // Move each file from manifest to completed folder
                    for (final var entry : manifest.files().entrySet()) {
                        final var filePath = Path.of(entry.getKey());
                        final var relative = rejectedDir.getParent().relativize(filePath);
                        scope.fork(() -> {
                            FileOperations.moveFileAtomically(filePath, completedDir.resolve(relative));
                            return null;
                        });
                    }
                    scope.join();
                    
                    final var relative = manifestsIncoming.relativize(path);
                    FileOperations.moveFileAtomically(path, manifestsLanded.resolve(relative));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        final var now = Instant.now();

        // Create a new manifest if conditions are met (e.g. file count threshold
        // reached)
        try (final var uploaded = Files.walk(uploadedDir);
                final var rejected = Files.walk(rejectedDir);
                final var dropped = Files.walk(droppedDir);
                final var failed = Files.walk(failedDir);) {
            final var paths = Stream.concat(Stream.concat(uploaded, rejected), Stream.concat(dropped, failed))
                    .filter(Files::isRegularFile).toList();
            final long totalFiles = paths.size();
            if (totalFiles >= 10_000) { // simplified condition for demonstration
                return;
            }

            if (now.isBefore(next)) {
                return;
            }

            final var map = paths.stream().collect(Collectors.toMap(path -> path.toString(), path -> path.toFile().length()));
            final var content = new Manifest(map);
            final var manifestFile = manifestsIncoming.resolve("manifest_" + formatter.format(now) + ".json");
            objectMapper.writeValue(manifestFile.toFile(), content);
        }

        next = now.plusMillis(monitorIntervalMillis);
    }

    public static record Manifest(Map<String, Long> files) {
    }
}
