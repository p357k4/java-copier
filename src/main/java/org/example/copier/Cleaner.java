package org.example.copier;

import java.nio.file.*;
import java.util.logging.Logger;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Cleaner {
    private static final Logger logger = Logger.getLogger(Cleaner.class.getName());

    public static void run(Config config) {
        final var manifestsUploadedDir = Paths.get(config.manifestsUploadedDir());
        final var completedUploadedDir = Paths.get(config.completedUploadedDir());
        final var completedRejectedDir = Paths.get(config.completedRejectedDir());
        final var completedDroppedDir = Paths.get(config.completedDroppedDir());
        final var completedFailedDir = Paths.get(config.completedFailedDir());
        final var manifestsCompletedUploadedDir = Paths.get(config.manifestsCompletedUploadedDir());
        final var manifestsCompletedRejectedDir = Paths.get(config.manifestsCompletedRejectedDir());
        final var manifestsCompletedDroppedDir = Paths.get(config.manifestsCompletedDroppedDir());
        final var manifestsCompletedFailedDir = Paths.get(config.manifestsCompletedFailedDir());
        final var manifestsCompletedUberDir = Paths.get(config.manifestsCompletedUberDir());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Files.list(manifestsUploadedDir)
                    .filter(Files::isRegularFile)
                    .forEach(uberManifestFile -> {
                        try {
                            final var mapper = new ObjectMapper();
                            final var uberManifest = mapper.readValue(uberManifestFile.toFile(), UberManifest.class);

                            moveFilesFromManifest(uberManifest.uploadedManifest(), completedUploadedDir);
                            moveFilesFromManifest(uberManifest.rejectedManifest(), completedRejectedDir);
                            moveFilesFromManifest(uberManifest.droppedManifest(), completedDroppedDir);
                            moveFilesFromManifest(uberManifest.failedManifest(), completedFailedDir);

                            Files.move(uberManifest.uploadedManifest(),
                                    manifestsCompletedUploadedDir.resolve(uberManifest.uploadedManifest().getFileName()),
                                    StandardCopyOption.ATOMIC_MOVE);
                            Files.move(uberManifest.rejectedManifest(),
                                    manifestsCompletedRejectedDir.resolve(uberManifest.rejectedManifest().getFileName()),
                                    StandardCopyOption.ATOMIC_MOVE);
                            Files.move(uberManifest.droppedManifest(),
                                    manifestsCompletedDroppedDir.resolve(uberManifest.droppedManifest().getFileName()),
                                    StandardCopyOption.ATOMIC_MOVE);
                            Files.move(uberManifest.failedManifest(),
                                    manifestsCompletedFailedDir.resolve(uberManifest.failedManifest().getFileName()),
                                    StandardCopyOption.ATOMIC_MOVE);

                            Files.move(uberManifestFile,
                                    manifestsCompletedUberDir.resolve(uberManifestFile.getFileName()),
                                    StandardCopyOption.ATOMIC_MOVE);

                            logger.info("Cleaned up files for uber manifest " + uberManifestFile.getFileName());
                        } catch (Exception e) {
                            logger.warning("Failed cleaning uber manifest " + uberManifestFile.getFileName() + ": " + e.getMessage());
                        }
                    });
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                logger.info("Cleaner interrupted, stopping.");
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.warning("Error in Cleaner: " + e.getMessage());
            }
        }
    }

    private static void moveFilesFromManifest(Path manifestFile, Path targetDir) {
        try {
            final var lines = Files.readAllLines(manifestFile);
            for (final var line : lines) {
                final var source = Paths.get(line);
                final var target = targetDir.resolve(source.getFileName());
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (IOException e) {
            logger.warning("Error moving files from manifest " + manifestFile.getFileName() + ": " + e.getMessage());
        }
    }
}
