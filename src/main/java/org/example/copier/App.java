package org.example.copier;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

public class App {
    private static final Logger logger = Logger.getLogger(App.class.getName());

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java App <componentName> <configFilePath>");
            System.exit(1);
        }
        final var componentName = args[0];
        final var configFilePath = args[1];

        try {
            final var objectMapper = new ObjectMapper();
            final var config = objectMapper.readValue(new File(configFilePath), AppConfig.class);

            final Component component;
            switch (componentName) {
                case "file-monitoring" -> component = new FileMonitoringComponent();
                case "file-filtering" -> component = new FileFilteringComponent();
                case "file-uploading" -> component = new FileUploadingComponent();
                case "manifest" -> component = new ManifestComponent();
                case "manifest-uploading" -> component = new ManifestUploadingComponent();
                case "cleaning" -> component = new CleaningComponent();
                default -> {
                    System.err.println("Unknown component: " + componentName);
                    return;
                }
            }

            // Run the chosen component using a virtual thread executor.
            try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                executor.submit(() -> component.run(config));
            }
        } catch (Exception e) {
            logger.severe("Error in App: " + e.getMessage());
        }
    }
}
