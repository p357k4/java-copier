package org.example.copier;

// Main.java
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.logging.Level;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final String CONFIG_FILE = "config.json";
    
    public static void main(String[] args) {
        if (args.length < 1) {
            LOGGER.severe("Usage: java Main <component-name>");
            System.exit(1);
        }
        
        try {
            final var component = args[0];
            final var config = loadConfiguration();
            
            // Create all necessary directories
            createDirectories(config);
            
            // Run the specified component
            switch (component) {
                case "incoming-file-monitoring" -> startIncomingFileMonitoring(config);
                case "file-filtering" -> startFileFiltering(config);
                case "file-uploading" -> startFileUploading(config);
                case "manifest" -> startManifest(config);
                case "manifest-uploading" -> startManifestUploading(config);
                case "cleaning" -> startCleaning(config);
                default -> {
                    LOGGER.severe("Unknown component: " + component);
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Application failed", e);
            System.exit(1);
        }
    }
    
    private static Configuration loadConfiguration() throws IOException {
        final var objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        final var configPath = Paths.get(CONFIG_FILE);
        try (final var reader = Files.newBufferedReader(configPath)) {
            return objectMapper.readValue(reader, Configuration.class);
        }
    }
    
    private static void createDirectories(Configuration config) throws IOException {
        for (final var directory : config.getAllDirectories()) {
            Files.createDirectories(directory);
            LOGGER.info("Created directory: " + directory);
        }
    }
    
    private static void startIncomingFileMonitoring(Configuration config) {
        LOGGER.info("Starting Incoming File Monitoring component");
        final var component = new IncomingFileMonitoring(config);
        
        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(component::run);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.info("Incoming File Monitoring component interrupted");
        }
    }
    
    private static void startFileFiltering(Configuration config) {
        LOGGER.info("Starting File Filtering component");
        final var component = new FileFiltering(config);
        
        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(component::run);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.info("File Filtering component interrupted");
        }
    }
    
    private static void startFileUploading(Configuration config) {
        LOGGER.info("Starting File Uploading component");
        final var component = new FileUploading(config);
        
        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(component::run);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.info("File Uploading component interrupted");
        }
    }
    
    private static void startManifest(Configuration config) {
        LOGGER.info("Starting Manifest component");
        final var component = new Manifest(config);
        
        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(component::run);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.info("Manifest component interrupted");
        }
    }
    
    private static void startManifestUploading(Configuration config) {
        LOGGER.info("Starting Manifest Uploading component");
        final var component = new ManifestUploading(config);
        
        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(component::run);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.info("Manifest Uploading component interrupted");
        }
    }
    
    private static void startCleaning(Configuration config) {
        LOGGER.info("Starting Cleaning component");
        final var component = new Cleaning(config);
        
        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(component::run);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.info("Cleaning component interrupted");
        }
    }
}
