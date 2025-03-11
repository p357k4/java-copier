package org.example.copier;

import java.nio.file.*;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.io.IOException;

public class ManifestCreator {
    private static final Logger logger = Logger.getLogger(ManifestCreator.class.getName());

    public static void run(Config config) {
        final var uploadedDir = Paths.get(config.uploadedDir());
        final var acceptedDir = Paths.get(config.acceptedDir());
        final var rejectedDir = Paths.get(config.rejectedDir());
        final var droppedDir = Paths.get(config.droppedDir());
        final var failedDir = Paths.get(config.failedDir());

        final var manifestsUploadedDir = Paths.get(config.manifestsUploadedDir());
        final var manifestsRejectedDir = Paths.get(config.manifestsRejectedDir());
        final var manifestsDroppedDir = Paths.get(config.manifestsDroppedDir());
        final var manifestsFailedDir = Paths.get(config.manifestsFailedDir());
        final var manifestsLandedDir = Paths.get(config.manifestsLandedDir());

        int fileCount = 0;
        long lastManifestTime = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                final var currentTime = System.currentTimeMillis();
                if ((currentTime - lastManifestTime >= TimeUnit.HOURS.toMillis(1)) || (fileCount >= 1000)) {
                    final var uploadedFiles = listFiles(uploadedDir);
                    final var rejectedFiles = listFiles(rejectedDir);
                    final var droppedFiles = listFiles(droppedDir);
                    final var failedFiles = listFiles(failedDir);

                    final var timestamp = System.currentTimeMillis();
                    final var uploadedManifest = manifestsUploadedDir.resolve("manifest_" + timestamp + ".txt");
                    final var rejectedManifest = manifestsRejectedDir.resolve("manifest_" + timestamp + ".txt");
                    final var droppedManifest = manifestsDroppedDir.resolve("manifest_" + timestamp + ".txt");
                    final var failedManifest = manifestsFailedDir.resolve("manifest_" + timestamp + ".txt");

                    Files.write(uploadedManifest, uploadedFiles);
                    Files.write(rejectedManifest, rejectedFiles);
                    Files.write(droppedManifest, droppedFiles);
                    Files.write(failedManifest, failedFiles);

                    // Create uber manifest record
                    final var uberManifest = new UberManifest(uploadedManifest, rejectedManifest, droppedManifest, failedManifest);
                    final var uberManifestPath = manifestsLandedDir.resolve("uber_manifest_" + timestamp + ".json");

                    final var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                    Files.writeString(uberManifestPath, mapper.writeValueAsString(uberManifest));

                    logger.info("Created manifests and uber manifest");
                    fileCount = 0;
                    lastManifestTime = currentTime;
                } else {
                    fileCount += Files.list(uploadedDir).toList().size();
                    fileCount += Files.list(acceptedDir).toList().size();
                    fileCount += Files.list(rejectedDir).toList().size();
                    fileCount += Files.list(droppedDir).toList().size();
                    fileCount += Files.list(failedDir).toList().size();
                }
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                logger.info("ManifestCreator interrupted, stopping.");
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.warning("Error writing manifest files: " + e.getMessage());
            }
        }
    }

    private static List<String> listFiles(Path directory) throws IOException {
        final var fileList = new ArrayList<String>();
        Files.list(directory)
            .filter(Files::isRegularFile)
            .forEach(path -> fileList.add(path.toString()));
        return fileList;
    }
}
