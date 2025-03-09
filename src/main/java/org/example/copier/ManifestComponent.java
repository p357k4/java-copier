package org.example.copier;
// File: ManifestComponent.java
import java.nio.file.*;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

public class ManifestComponent implements Runnable {
    private static final Logger logger = Logger.getLogger(ManifestComponent.class.getName());
    
    private final Path confirmedDir = Paths.get("files/confirmed");
    private final Path acceptedDir = Paths.get("files/accepted");
    private final Path rejectedDir = Paths.get("files/rejected");
    private final Path droppedDir = Paths.get("files/dropped");
    private final Path failedDir = Paths.get("files/failed");
    
    private final Path manifestsConfirmedDir = Paths.get("manifests/confirmed");
    private final Path manifestsAcceptedDir = Paths.get("manifests/accepted");
    private final Path manifestsRejectedDir = Paths.get("manifests/rejected");
    private final Path manifestsDroppedDir = Paths.get("manifests/dropped");
    private final Path manifestsFailedDir = Paths.get("manifests/failed");
    private final Path manifestsLandedDir = Paths.get("manifests/landed");
    
    // Either an hour or when 1000 files are reached.
    private final int manifestFileCountLimit = 1000;
    private final long manifestIntervalMillis = TimeUnit.HOURS.toMillis(1);
    
    @Override
    public void run() {
        long lastManifestTime = System.currentTimeMillis();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final var totalFiles = Files.list(confirmedDir).count() +
                                       Files.list(acceptedDir).count() +
                                       Files.list(rejectedDir).count() +
                                       Files.list(droppedDir).count() +
                                       Files.list(failedDir).count();
                if (totalFiles >= manifestFileCountLimit ||
                    System.currentTimeMillis() - lastManifestTime >= manifestIntervalMillis) {
                    
                    final var confirmedManifest = createManifest(confirmedDir);
                    final var acceptedManifest = createManifest(acceptedDir);
                    final var rejectedManifest = createManifest(rejectedDir);
                    final var droppedManifest = createManifest(droppedDir);
                    final var failedManifest = createManifest(failedDir);
                    
                    final var timestamp = System.currentTimeMillis();
                    final var confirmedManifestFile = manifestsConfirmedDir.resolve("confirmed_manifest_" + timestamp + ".json");
                    final var acceptedManifestFile = manifestsAcceptedDir.resolve("accepted_manifest_" + timestamp + ".json");
                    final var rejectedManifestFile = manifestsRejectedDir.resolve("rejected_manifest_" + timestamp + ".json");
                    final var droppedManifestFile = manifestsDroppedDir.resolve("dropped_manifest_" + timestamp + ".json");
                    final var failedManifestFile = manifestsFailedDir.resolve("failed_manifest_" + timestamp + ".json");
                    
                    Util.writeJson(confirmedManifestFile, confirmedManifest);
                    Util.writeJson(acceptedManifestFile, acceptedManifest);
                    Util.writeJson(rejectedManifestFile, rejectedManifest);
                    Util.writeJson(droppedManifestFile, droppedManifest);
                    Util.writeJson(failedManifestFile, failedManifest);
                    
                    // Create the uber manifest record.
                    final var uberManifest = new UberManifest(
                        confirmedManifestFile.toString(),
                        acceptedManifestFile.toString(),
                        rejectedManifestFile.toString(),
                        droppedManifestFile.toString(),
                        failedManifestFile.toString()
                    );
                    final var uberManifestFile = manifestsLandedDir.resolve("uber_manifest_" + timestamp + ".json");
                    Util.writeJson(uberManifestFile, uberManifest);
                    
                    lastManifestTime = System.currentTimeMillis();
                }
            } catch (IOException e) {
                logger.warning("Error creating manifest: " + e.getMessage());
            }
            try {
                Thread.sleep(10000); // Poll every 10 seconds.
            } catch (InterruptedException e) {
                logger.info("ManifestComponent interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private List<String> createManifest(Path dir) throws IOException {
        final var filesList = new ArrayList<String>();
        try (final var stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile).forEach(file -> filesList.add(file.toString()));
        }
        return filesList;
    }
}
