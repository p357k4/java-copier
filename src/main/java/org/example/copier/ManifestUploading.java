package org.example.copier;

// ManifestUploading.java
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class ManifestUploading {
    private static final Logger LOGGER = Logger.getLogger(ManifestUploading.class.getName());
    private static final long SCAN_INTERVAL_SECONDS = 30;
    
    private final Configuration config;
    private final GcsService gcsService;
    private final ObjectMapper objectMapper;
    
    public ManifestUploading(Configuration config) {
        this.config = config;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        
        try {
            this.gcsService = new GcsService(config.gcsBucket(), config.gcsKeyFile());
            LOGGER.info("Manifest Uploading initialized to watch folder: " + config.manifestsLandedFolder());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize GCS service", e);
            throw new RuntimeException("Failed to initialize GCS service", e);
        }
    }
    
    public void run() {
        LOGGER.info("Manifest Uploading started");
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                processManifests();
                TimeUnit.SECONDS.sleep(SCAN_INTERVAL_SECONDS);
            } catch (InterruptedException e) {
                FileUtils.handleInterruptedException(e);
                break;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing manifests", e);
            }
        }
        
        LOGGER.info("Manifest Uploading stopped");
    }
    
    private void processManifests() throws IOException {
        LOGGER.info("Scanning manifests folder: " + config.manifestsLandedFolder());
        
        try (final var files = Files.list(config.manifestsLandedFolder())) {
            files.filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".json"))
                .forEach(this::processManifest);
        }
    }
    
    private void processManifest(Path manifestPath) {
        final var fileName = manifestPath.getFileName().toString();
        LOGGER.info("Processing manifest: " + fileName);
        
        try {
            // Read the manifest file
            final var manifest = objectMapper.readValue(manifestPath.toFile(), ManifestData.class);
            
            // Create uploaded files manifest
            final var timestamp = FileUtils.getCurrentTimestampUtc();
            final var uploadedFiles = manifest.getFiles().stream()
                    .filter(path -> path.contains("files/uploaded"))
                    .collect(Collectors.toList());
            
            final var uploadedManifest = new ManifestData(timestamp, uploadedFiles);
            final var uploadedManifestFileName = "uploaded_" + fileName;
            final var uploadedManifestPath = Path.of(uploadedManifestFileName);
            
            // Write the uploaded files manifest to a temporary file
            objectMapper.writeValue(uploadedManifestPath.toFile(), uploadedManifest);
            
            // Upload the uploaded files manifest
            final var destinationPath = "manifests/incoming/" + uploadedManifestFileName;
            
            int attempts = 0;
            boolean success = false;
            
            while (attempts < config.retryLimit() && !success) {
                attempts++;
                LOGGER.info("Upload attempt " + attempts + " for manifest: " + uploadedManifestFileName);
                
                success = gcsService.uploadFile(uploadedManifestPath, destinationPath);
                
                if (!success) {
                    try {
                        TimeUnit.SECONDS.sleep(2 * attempts); // Exponential backoff
                    } catch (InterruptedException e) {
                        FileUtils.handleInterruptedException(e);
                        break;
                    }
                }
            }
            
            // Clean up the temporary file
            Files.deleteIfExists(uploadedManifestPath);
            
            Path destinationPath;
            if (success) {
                destinationPath = config.manifestsUploadedFolder().resolve(fileName);
                LOGGER.info("Manifest uploaded successfully: " + fileName);
            } else if (attempts >= config.retryLimit()) {
                destinationPath = config.manifestsDroppedFolder().resolve(fileName);
                LOGGER.warning("Failed to upload manifest after " + attempts + " attempts: " + fileName);
            } else {
                destinationPath = config.manifestsFailedFolder().resolve(fileName);
                LOGGER.warning("Manifest upload interrupted: " + fileName);
            }
            
            try {
                FileUtils.moveFileAtomically(manifestPath, destinationPath);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to move manifest file: " + fileName, e);
            }
            
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error processing manifest: " + fileName, e);
            
            try {
                final var failedPath = config.manifestsFailedFolder().resolve(fileName);
                FileUtils.moveFileAtomically(manifestPath, failedPath);
            } catch (IOException moveEx) {
                LOGGER.log(Level.SEVERE, "Failed to move manifest to failed folder: " + fileName, moveEx);
            }
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