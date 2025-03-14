package org.example.copier;

// Manifest.java
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;

public class Manifest {
    private static final Logger LOGGER = Logger.getLogger(Manifest.class.getName());
    
    private final Configuration config;
    private final ObjectMapper objectMapper;
    private final AtomicInteger fileCount;
    
    public Manifest(Configuration config) {
        this.config = config;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.fileCount = new AtomicInteger(0);
        LOGGER.info("Manifest component initialized");
    }
    
    public void run() {
        LOGGER.info("Manifest component started");
        
        final var lastManifestTime = System.currentTimeMillis();
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final var currentTime = System.currentTimeMillis();
                final var elapsedHours = (currentTime - lastManifestTime) / (60 * 60 * 1000);
                
                // Generate manifest if enough files or enough time has passed
                if (fileCount.get() >= 1000 || elapsedHours >= 1) {
                    generateManifest();
                    fileCount.set(0);
                }
                
                // Scan directories and update file count
                scanDirectories();
                
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
                FileUtils.handleInterruptedException(e);
                break;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in manifest processing", e);
            }
        }
        
        LOGGER.info("Manifest component stopped");
    }
    
    private void scanDirectories() throws IOException {
        LOGGER.info("Scanning directories for manifest");
        
        int count = 0;
        final var rootDir = config.incomingFolder().getParent();
        
        try (final var paths = Files.walk(rootDir)) {
            count = (int) paths.filter(Files::isRegularFile).count();
        }
        
        fileCount.set(count);
        LOGGER.info("Found " + count + " files in monitored directories");
    }
    
    private void generateManifest() throws IOException {
        LOGGER.info("Generating manifest");
        
        final var timestamp = FileUtils.getCurrentTimestampUtc();
        final var manifestFilePath = config.manifestsLandedFolder().resolve("manifest_" + timestamp + ".json");
        
        final var filePaths = new ArrayList<String>();
        final var rootDir = config.incomingFolder().getParent();
        
        try (final var paths = Files.walk(rootDir)) {
            paths.filter(Files::isRegularFile)
                .map(Path::toString)
                .forEach(filePaths::add);
        }
        
        final var manifest = new ManifestData(timestamp, filePaths);
        
        try {
            Files.createDirectories(manifestFilePath.getParent());
            objectMapper.writeValue(manifestFilePath.toFile(), manifest);
            LOGGER.info("Manifest generated: " + manifestFilePath);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to write manifest", e);
            throw e;
        }
    }
    
    private static class ManifestData {
        private String timestamp;
        private List<String> files;
        
        public ManifestData() {
        }
        
        public ManifestData(String timestamp, List<String> files) {
            this.timestamp = timestamp;
            this.files = files;
        }
        
        public String getTimestamp() {
            return timestamp;
        }
        
        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }
        
        public List<String> getFiles() {
            return files;
        }
        
        public void setFiles(List<String> files) {
            this.files = files;
        }
    }
}
