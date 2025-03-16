package org.example.copier;

// Main.java
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());
    private static final String CONFIG_FILE = "config.json";

    public static void main(String[] args) {
        final var objectMapper = new ObjectMapper();
        final Configuration configuration;
        try {
            configuration = objectMapper.readValue(Path.of(CONFIG_FILE).toFile(), Configuration.class);
        } catch (IOException e) {
            logger.severe("Failed to load configuration: " + e.getMessage());
            return;
        }
        // Create directories as specified in configuration.
        createDirectories(configuration);

        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(new IncomingFileMonitor(configuration));
            executor.submit(new FileFilterComponent(configuration));
            executor.submit(new FileUploadComponent(configuration));
            executor.submit(new ManifestComponent(configuration));
            executor.submit(new ManifestUploadComponent(configuration));
            executor.submit(new CleaningComponent(configuration));

            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("Main thread interrupted.");
        }
    }

    private static void createDirectories(Configuration configuration) {
        final var dirs = new String[] {
            configuration.incoming(),
            configuration.landed(),
            configuration.accepted(),
            configuration.rejected(),
            configuration.failed(),
            configuration.dropped(),
            configuration.completed(),
            configuration.manifestLanded(),
            configuration.manifestDropped(),
            configuration.manifestUploaded(),
            configuration.manifestFailed(),
            configuration.manifestIncoming(),
            configuration.manifestCompleted(),
            configuration.gcs(),
            configuration.uploaded()
        };

        for (final var dirStr : dirs) {
            try {
                final var dirPath = Paths.get(dirStr);
                Files.createDirectories(dirPath);
                logger.info("Created directory: " + dirPath.toAbsolutePath());
            } catch (IOException e) {
                logger.severe("Failed to create directory " + dirStr + ": " + e.getMessage());
            }
        }
    }
}
