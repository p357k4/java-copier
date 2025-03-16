package org.example.copier;

import java.nio.file.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.StructuredTaskScope;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ManifestUploadingComponent implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(ManifestUploadingComponent.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

    private final Path manifestsFailed;
    private final Path manifestsUploaded;
    private final Path manifestsTemporary;
    private final Path manifestsLanded;

    public ManifestUploadingComponent(Configuration config) {
        this.manifestsLanded = Paths.get(config.manifestsLandedDir());
        this.manifestsTemporary = Paths.get(config.manifestsTemporaryDir());
        this.manifestsUploaded = Paths.get(config.manifestsUploadedDir());
        this.manifestsFailed = Paths.get(config.manifestsFailedDir());
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        try (final var paths = Files.walk(manifestsLanded)) {
            paths.filter(Files::isRegularFile).forEach(path -> scope.fork(() -> {
                final var relative = manifestsLanded.relativize(path);

                try {
                    final var now = Instant.now();
                    final var manifest = objectMapper.readValue(path.toFile(), ManifestCreatorComponent.Manifest.class);
                    // Create temporary manifest in memory (here simply reuse the same manifest)
                    final var filteredManifest = new ManifestCreatorComponent.Manifest(manifest.files());
                    final var filteredPath = manifestsTemporary
                            .resolve("filtered_manifest_" + formatter.format(now) + ".json");
                    Files.writeString(filteredPath, objectMapper.writeValueAsString(filteredManifest),
                            StandardCharsets.UTF_8);
                    FileOperations.moveFileAtomically(path, manifestsUploaded.resolve(relative));
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Error uploading manifest: " + path, ex);
                    FileOperations.moveFileAtomically(path, manifestsFailed.resolve(relative));
                }
                return null;
            }));
        }
    }
}
