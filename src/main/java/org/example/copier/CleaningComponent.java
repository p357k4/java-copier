package org.example.copier;
// File: CleaningComponent.java
import java.nio.file.*;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.List;

public class CleaningComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(CleaningComponent.class.getName());
    
    private final Path manifestsUploadedDir = Paths.get("manifests/uploaded");
    private final Path filesCompletedDir = Paths.get("files/completed");
    private final Path manifestsCompletedDir = Paths.get("manifests/completed");
    
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (final var files = Files.list(manifestsUploadedDir)) {
                files.filter(Files::isRegularFile).forEach(file -> {
                    try {
                        // Read the uber manifest record.
                        final var uberManifest = Util.readJson(file, UberManifest.class);
                        // Process each manifest file listed in the uber manifest.
                        moveManifestFiles(uberManifest.confirmedManifestPath(), "uploaded", "confirmed");
                        moveManifestFiles(uberManifest.acceptedManifestPath(), "uploaded", "accepted");
                        moveManifestFiles(uberManifest.rejectedManifestPath(), "rejected", "rejected");
                        moveManifestFiles(uberManifest.droppedManifestPath(), "dropped", "dropped");
                        moveManifestFiles(uberManifest.failedManifestPath(), "failed", "failed");
                        // Finally, move the uber manifest file itself.
                        Util.atomicMove(file, manifestsCompletedDir.resolve(file.getFileName()));
                    } catch (IOException e) {
                        logger.warning("Error cleaning manifest file " + file + ": " + e.getMessage());
                    }
                });
            } catch (IOException e) {
                logger.warning("Error listing files in " + manifestsUploadedDir + ": " + e.getMessage());
            }
            try {
                Thread.sleep(10000); // Poll every 10 seconds.
            } catch (InterruptedException e) {
                logger.info("CleaningComponent interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private void moveManifestFiles(String manifestFilePath, String sourceSubDir, String targetSubDir) {
        try {
            final var manifestPath = Paths.get(manifestFilePath);
            // Read the manifest content (assumed to be a JSON array of file paths).
            final List<Object> fileList = Util.readJson(manifestPath, List.class);
            for (final Object pathObj : fileList) {
                final var source = Paths.get(pathObj.toString());
                final var target = Paths.get("files/completed").resolve(targetSubDir).resolve(source.getFileName());
                Util.atomicMove(source, target);
            }
            // Move the manifest file itself.
            final var manifestTargetDir = Paths.get("manifests/completed").resolve(targetSubDir);
            Util.atomicMove(manifestPath, manifestTargetDir.resolve(manifestPath.getFileName()));
        } catch (IOException e) {
            logger.warning("Error moving manifest files for " + manifestFilePath + ": " + e.getMessage());
        }
    }
}
