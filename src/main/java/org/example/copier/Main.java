package org.example.copier;
// File: Main.java
import java.nio.file.*;
import java.io.IOException;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());
    
    public static void main(String[] args) {
        if (args.length < 2) {
            logger.severe("Usage: java Main <component> <configFilePath>");
            System.exit(1);
        }
        final var componentArg = args[0];
        final var configFilePath = Paths.get(args[1]);
        Config config = null;
        try {
            config = Util.readJson(configFilePath, Config.class);
        } catch (IOException e) {
            logger.severe("Failed to load configuration: " + e.getMessage());
            System.exit(1);
        }
        
        // Use a switch expression with pattern matching to select the component.
        final Runnable component = switch (componentArg) {
            case "monitoring" -> new FileMonitoringComponent();
            case "filtering" -> new FileFilteringComponent();
            case "uploading" -> new FileUploadingComponent();
            case "manifest" -> new ManifestComponent();
            case "manifestUploading" -> new ManifestUploadingComponent();
            case "cleaning" -> new CleaningComponent();
            default -> {
                logger.severe("Unknown component: " + componentArg);
                yield () -> {};
            }
        };
        
        // Run the selected component in a virtual thread.
        Thread.startVirtualThread(component);
    }
}
