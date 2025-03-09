package org.example.copier;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.logging.Logger;

public class ManifestComponent implements Component {
    private static final Logger logger = Logger.getLogger(ManifestComponent.class.getName());
    private static final int MANIFEST_THRESHOLD = 1000;
    private static final long ONE_HOUR_MILLIS = 3600_000;

    @Override
    public void run(AppConfig config) {
        final var confirmedPath = Paths.get(config.confirmedDir());
        final var acceptedPath = Paths.get(config.acceptedDir());
        final var rejectedPath = Paths.get(config.rejectedDir());
        final var droppedPath = Paths.get(config.droppedDir());
        final var failedPath = Paths.get(config.failedDir());

        final var manifestsConfirmedPath = Paths.get(config.manifestsConfirmedDir());
        final var manifestsAcceptedPath = Paths.get(config.manifestsAcceptedDir());
        final var manifestsRejectedPath = Paths.get(config.manifestsRejectedDir());
        final var manifestsDroppedPath = Paths.get(config.manifestsDroppedDir());
        final var manifestsFailedPath = Paths.get(config.manifestsFailedDir());
        final var uberManifestDir = Paths.get(config.uberManifestDir());

        long lastManifestTime = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                final var confirmedFiles = listFiles(confirmedPath);
                final var acceptedFiles = listFiles(acceptedPath);
                final var rejectedFiles = listFiles(rejectedPath);
                final var droppedFiles = listFiles(droppedPath);
                final var failedFiles = listFiles(failedPath);

                if (confirmedFiles.size() >= MANIFEST_THRESHOLD ||
                    acceptedFiles.size() >= MANIFEST_THRESHOLD ||
                    rejectedFiles.size() >= MANIFEST_THRESHOLD ||
                    droppedFiles.size() >= MANIFEST_THRESHOLD ||
                    failedFiles.size() >= MANIFEST_THRESHOLD ||
                    System.currentTimeMillis() - lastManifestTime >= ONE_HOUR_MILLIS) {

                    final var timestamp = System.currentTimeMillis();
                    final var confirmedManifest = manifestsConfirmedPath.resolve("manifest_" + timestamp + ".txt");
                    final var acceptedManifest = manifestsAcceptedPath.resolve("manifest_" + timestamp + ".txt");
                    final var rejectedManifest = manifestsRejectedPath.resolve("manifest_" + timestamp + ".txt");
                    final var droppedManifest = manifestsDroppedPath.resolve("manifest_" + timestamp + ".txt");
                    final var failedManifest = manifestsFailedPath.resolve("manifest_" + timestamp + ".txt");

                    writeManifest(confirmedManifest, confirmedFiles);
                    writeManifest(acceptedManifest, acceptedFiles);
                    writeManifest(rejectedManifest, rejectedFiles);
                    writeManifest(droppedManifest, droppedFiles);
                    writeManifest(failedManifest, failedFiles);

                    final var uberManifest = uberManifestDir.resolve("uber_manifest_" + timestamp + ".json");
                    final var uberContent = "{\n" +
                        "  \"uploadedTargetManifest\": \"" + confirmedManifest.toString() + "\",\n" +
                        "  \"uploadedSourceManifest\": \"" + acceptedManifest.toString() + "\",\n" +
                        "  \"rejectedManifest\": \"" + rejectedManifest.toString() + "\",\n" +
                        "  \"droppedManifest\": \"" + droppedManifest.toString() + "\",\n" +
                        "  \"failedManifest\": \"" + failedManifest.toString() + "\"\n" +
                        "}";
                    Files.writeString(uberManifest, uberContent);
                    logger.info("Created manifests and uber manifest.");
                    lastManifestTime = System.currentTimeMillis();
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.severe("I/O error in ManifestComponent: " + e.getMessage());
            } catch (Exception e) {
                logger.severe("Error in ManifestComponent: " + e.getMessage());
            }
        }
    }

    private List<Path> listFiles(Path dir) throws IOException {
        try (final var stream = Files.list(dir)) {
            return stream.filter(Files::isRegularFile).toList();
        }
    }

    private void writeManifest(Path manifestFile, List<Path> files) throws IOException {
        try (final var writer = Files.newBufferedWriter(manifestFile)) {
            for (final var file : files) {
                writer.write(file.toString());
                writer.newLine();
            }
        }
    }
}
