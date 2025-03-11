package org.example.copier;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.logging.Logger;

public class FileCopyApp {
    private static final Logger logger = Logger.getLogger(FileCopyApp.class.getName());

    public static void main(String[] args) {
        if (args.length < 2) {
            logger.severe("Usage: java FileCopyApp <component> <config_file_path>");
            System.exit(1);
        }
        final var component = args[0];
        final var configFilePath = args[1];

        final var config = loadConfig(Paths.get(configFilePath));
        logger.info("Configuration loaded successfully");

        // Run the selected component using a switch expression
        switch (component) {
            case "monitor" -> FileMonitor.run(config);
            case "filter" -> FileFilter.run(config);
            case "upload" -> FileUploader.run(config);
            case "manifest" -> ManifestCreator.run(config);
            case "manifestUpload" -> ManifestUploader.run(config);
            case "clean" -> Cleaner.run(config);
            default -> {
                logger.severe("Unknown component: " + component);
                System.exit(1);
            }
        }
    }

    private static Config loadConfig(Path configPath) {
        final var mapper = new ObjectMapper();
        try {
            return mapper.readValue(configPath.toFile(), Config.class);
        } catch (IOException e) {
            logger.severe("Failed to load configuration: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
