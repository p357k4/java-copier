package org.example.copier;

import java.nio.file.*;
import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(final String[] args) {
        final var configFile = Paths.get("config.json");
        final Configuration config;
        try {
            final var mapper = new ObjectMapper();
            config = mapper.readValue(configFile.toFile(), Configuration.class);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Failed to load configuration", ex);
            return;
        }

        // Create all configuration directories on startup.
        final var dirs = new String[] { config.filesIncomingDir(), config.filesLandedDir(), config.filesCompressedDir(),
                config.filesCompletedDir(), config.filesAcceptedDir(), config.filesRejectedDir(),
                config.filesFailedDir(), config.filesDroppedDir(), config.filesUploadedDir(),
                config.manifestsIncomingDir(), config.manifestsLandedDir(), config.manifestsUploadedDir(),
                config.manifestsFailedDir(), config.manifestsRegisteredDir(), config.manifestsCompletedDir(),
                config.manifestsTemporaryDir(), config.gcpDir() };
        for (final var dir : dirs) {
            try {
                Files.createDirectories(Paths.get(dir));
            } catch (IOException ex) {
                logger.log(Level.WARNING, "Failed to create directory: " + dir, ex);
            }
        }

        // Launch each component in its own thread.
        try (final var executor = Executors.newWorkStealingPool()) {
            final var interval = config.monitorIntervalMillis();
            executor.execute(() -> ComponentRunner.runComponent(new IncomingFileMonitoring(config), interval));
            executor.execute(() -> ComponentRunner.runComponent(new FileDecompressComponent(config), interval));
            executor.execute(() -> ComponentRunner.runComponent(new FileFilteringComponent(config), interval));
            executor.execute(() -> ComponentRunner.runComponent(new FileUploadingComponent(config), interval));
            executor.execute(() -> ComponentRunner.runComponent(new ManifestCreatorComponent(config), interval));
            executor.execute(() -> ComponentRunner.runComponent(new ManifestUploadingComponent(config), interval));
            executor.execute(() -> ComponentRunner.runComponent(new ManifestRegistratingComponent(config), interval));
            executor.execute(() -> ComponentRunner.runComponent(new CleaningComponent(config), interval));
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
