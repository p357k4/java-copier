package org.example.copier;

import java.nio.file.*;
import java.util.logging.Logger;

public class FileUploadingComponent implements Component {
    private static final Logger logger = Logger.getLogger(FileUploadingComponent.class.getName());
    private static final int RETRY_LIMIT = 5;

    @Override
    public void run(AppConfig config) {
        final var acceptedPath = Paths.get(config.acceptedDir());
        final var uploadedPath = Paths.get(config.uploadedDir());
        final var confirmedPath = Paths.get(config.confirmedDir());
        final var droppedPath = Paths.get(config.droppedDir());
        final var uploader = new GCPUploader(config.gcpKeyFilePath());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                try (final var stream = Files.list(acceptedPath)) {
                    final var files = stream.filter(Files::isRegularFile).toList();
                    for (final var file : files) {
                        boolean success = false;
                        int attempts = 0;
                        long backoff = 1000; // initial backoff of 1 second
                        while (attempts < RETRY_LIMIT && !success) {
                            try {
                                uploader.upload(file);
                                success = true;
                            } catch (Exception e) {
                                attempts++;
                                logger.warning("Upload failed for " + file + ", attempt " + attempts);
                                Thread.sleep(backoff);
                                backoff *= 2;
                            }
                        }
                        if (success) {
                            final var confirmationData = "{\"source\":\"" + file.toString() +
                                    "\", \"target\":\"" + uploadedPath.resolve(file.getFileName()).toString() + "\"}";
                            final var confirmationFile = confirmedPath.resolve(file.getFileName().toString() + ".json");
                            try (final var writer = Files.newBufferedWriter(confirmationFile)) {
                                writer.write(confirmationData);
                            }
                            final var target = uploadedPath.resolve(file.getFileName());
                            Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            logger.info("Uploaded and moved file " + file);
                        } else {
                            final var target = droppedPath.resolve(file.getFileName());
                            Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                            logger.warning("Upload failed after retries; moved file " + file + " to dropped");
                        }
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.severe("Error in FileUploadingComponent: " + e.getMessage());
            }
        }
    }
}
