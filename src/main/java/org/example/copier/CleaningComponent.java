package org.example.copier;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CleaningComponent implements Component {
    private static final Logger logger = Logger.getLogger(CleaningComponent.class.getName());

    @Override
    public void run(AppConfig config) {
        final var manifestsUploadedDir = Paths.get(config.manifestsUploadedDir());
        final var manifestsCompletedDir = Paths.get(config.manifestsCompletedDir());
        final var filesCompletedUploadedDir = Paths.get(config.completedUploadedDir());
        final var filesCompletedRejectedDir = Paths.get(config.completedRejectedDir());
        final var filesCompletedDroppedDir = Paths.get(config.completedDroppedDir());
        final var filesCompletedFailedDir = Paths.get(config.completedFailedDir());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                try (final var stream = Files.list(manifestsUploadedDir)) {
                    final var uberManifests = stream
                        .filter(p -> p.getFileName().toString().startsWith("uber_manifest_"))
                        .toList();
                    for (final var uberManifest : uberManifests) {
                        final var objectMapper = new ObjectMapper();
                        final var content = Files.readString(uberManifest);
                        final Map<?, ?> manifestMap = objectMapper.readValue(content, Map.class);

                        moveFilesFromManifest((String) manifestMap.get("uploadedTargetManifest"), filesCompletedUploadedDir);
                        moveFilesFromManifest((String) manifestMap.get("uploadedSourceManifest"), filesCompletedUploadedDir);
                        moveFilesFromManifest((String) manifestMap.get("rejectedManifest"), filesCompletedRejectedDir);
                        moveFilesFromManifest((String) manifestMap.get("droppedManifest"), filesCompletedDroppedDir);
                        moveFilesFromManifest((String) manifestMap.get("failedManifest"), filesCompletedFailedDir);

                        moveManifestFile((String) manifestMap.get("uploadedTargetManifest"), config.manifestsCompletedDir());
                        moveManifestFile((String) manifestMap.get("uploadedSourceManifest"), config.manifestsCompletedDir());
                        moveManifestFile((String) manifestMap.get("rejectedManifest"), config.manifestsCompletedDir());
                        moveManifestFile((String) manifestMap.get("droppedManifest"), config.manifestsCompletedDir());
                        moveManifestFile((String) manifestMap.get("failedManifest"), config.manifestsCompletedDir());

                        final var targetUber = Paths.get(config.manifestsCompletedDir()).resolve(uberManifest.getFileName());
                        Files.move(uberManifest, targetUber, StandardCopyOption.ATOMIC_MOVE);
                        logger.info("Cleaned uber manifest " + uberManifest);
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.severe("I/O error in CleaningComponent: " + e.getMessage());
            } catch (Exception e) {
                logger.severe("Error in CleaningComponent: " + e.getMessage());
            }
        }
    }

    private void moveFilesFromManifest(String manifestFilePath, Path targetDir) throws IOException {
        final var manifestFile = Paths.get(manifestFilePath);
        try (final var stream = Files.lines(manifestFile)) {
            stream.forEach(line -> {
                try {
                    final var source = Paths.get(line.trim());
                    final var target = targetDir.resolve(source.getFileName());
                    Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    logger.warning("Failed to move file from manifest: " + e.getMessage());
                }
            });
        }
    }

    private void moveManifestFile(String manifestFilePath, String completedDir) throws IOException {
        final var source = Paths.get(manifestFilePath);
        final var target = Paths.get(completedDir).resolve(source.getFileName());
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    }
}
