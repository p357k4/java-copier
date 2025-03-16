package org.example.copier;

import java.nio.file.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.StructuredTaskScope;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ManifestRegistratingComponent implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(ManifestRegistratingComponent.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Path manifestsRegistered;
    private final Path manifestsUploaded;
    private final Path manifestsFailed;

    public ManifestRegistratingComponent(Configuration config) {
        this.manifestsUploaded = Paths.get(config.manifestsUploadedDir());
        this.manifestsFailed = Paths.get(config.manifestsFailedDir());
        this.manifestsRegistered = Paths.get(config.manifestsRegisteredDir());
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        try (final var paths = Files.walk(manifestsUploaded)) {
            paths.filter(Files::isRegularFile).forEach(path -> scope.fork(() -> {
                final var relative = manifestsUploaded.relativize(path);

                try {
                    final var json = Files.readString(path, StandardCharsets.UTF_8);
                    final var manifest = objectMapper.readValue(json, ManifestCreatorComponent.Manifest.class);
                    int uploadedCount = manifest.files().size();
                    int failedCount = 0; // dummy values for demonstration
                    int droppedCount = 0;
                    int rejectedCount = 0;
                    logger.info("Registering manifest: " + relative + " | Uploaded: " + uploadedCount + ", Failed: "
                            + failedCount + ", Dropped: " + droppedCount + ", Rejected: " + rejectedCount);
                    FileOperations.moveFileAtomically(path, manifestsRegistered.resolve(relative));
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Error registering manifest: " + path, ex);
                    FileOperations.moveFileAtomically(path, manifestsFailed.resolve(relative));
                }
                return null;
            }));
        }
    }
}
