package org.example.copier;

import java.nio.file.*;
import java.util.logging.Logger;

public class ManifestUploadingComponent implements Component {
    private static final Logger logger = Logger.getLogger(ManifestUploadingComponent.class.getName());
    private static final int RETRY_LIMIT = 5;

    @Override
    public void run(AppConfig config) {
        final var uberManifestDir = Paths.get(config.uberManifestDir());
        final var manifestsIncomingDir = Paths.get(config.manifestsIncomingDir());
        final var manifestsUploadedDir = Paths.get(config.manifestsUploadedDir());
        final var manifestsDroppedDir = Paths.get(config.manifestsDroppedDir());
        final var uploader = new GCPUploader(config.gcpKeyFilePath());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                try (final var stream = Files.list(uberManifestDir)) {
                    final var uberManifests = stream
                        .filter(p -> p.getFileName().toString().startsWith("uber_manifest_"))
                        .toList();
                    for (final var uberManifest : uberManifests) {
                        boolean success = false;
                        int attempts = 0;
                        long backoff = 1000;
                        while (attempts < RETRY_LIMIT && !success) {
                            try {
                                uploader.upload(uberManifest);
                                success = true;
                            } catch (Exception e) {
                                attempts++;
                                logger.warning("Upload failed for uber manifest " + uberManifest + ", attempt " + attempts);
                                Thread.sleep(backoff);
                                backoff *= 2;
                            }
                        }
                        if (success) {
                            final var target = manifestsUploadedDir.resolve(uberManifest.getFileName());
                            Files.move(uberManifest, target, StandardCopyOption.ATOMIC_MOVE);
                            logger.info("Uploaded uber manifest and moved to " + target);
                        } else {
                            final var target = manifestsDroppedDir.resolve(uberManifest.getFileName());
                            Files.move(uberManifest, target, StandardCopyOption.ATOMIC_MOVE);
                            logger.warning("Failed to upload uber manifest after retries; moved to dropped: " + uberManifest);
                        }
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.severe("Error in ManifestUploadingComponent: " + e.getMessage());
            }
        }
    }
}
