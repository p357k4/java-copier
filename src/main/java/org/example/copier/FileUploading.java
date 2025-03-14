package org.example.copier;

// FileUploading.java
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

public class FileUploading {
    private static final Logger LOGGER = Logger.getLogger(FileUploading.class.getName());
    private static final long SCAN_INTERVAL_SECONDS = 10;
    
    private final Configuration config;
    private final GcsService gcsService;
    
    public FileUploading(Configuration config) {
        this.config = config;
        try {
            this.gcsService = new GcsService(config.gcsBucket(), config.gcsKeyFile());
            LOGGER.info("File Uploading initialized to watch folder: " + config.acceptedFolder());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize GCS service", e);
            throw new RuntimeException("Failed to initialize GCS service", e);
        }
    }
    
    public void run() {
        LOGGER.info("File Uploading started");
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                processAcceptedFiles();
                TimeUnit.SECONDS.sleep(SCAN_INTERVAL_SECONDS);
            } catch (InterruptedException e) {
                FileUtils.handleInterruptedException(e);
                break;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing accepted files", e);
            }
        }
        
        LOGGER.info("File Uploading stopped");
    }
    
    private void processAcceptedFiles() throws IOException {
        LOGGER.info("Scanning accepted folder: " + config.acceptedFolder());
        
        try (final var files = Files.list(config.acceptedFolder())) {
            files.filter(Files::isRegularFile).forEach(this::uploadFile);
        }
    }
    
    private void uploadFile(Path filePath) {
        final var fileName = filePath.getFileName().toString();
        LOGGER.info("Uploading file: " + fileName);
        
        int attempts = 0;
        boolean success = false;
        
        while (attempts < config.retryLimit() && !success) {
            attempts++;
            LOGGER.info("Upload attempt " + attempts + " for file: " + fileName);
            
            final var destinationPath = "files/uploaded/" + fileName;
            success = gcsService.uploadFile(filePath, destinationPath);
            
            if (!success) {
                try {
                    TimeUnit.SECONDS.sleep(2 * attempts); // Exponential backoff
                } catch (InterruptedException e) {
                    FileUtils.handleInterruptedException(e);
                    break;
                }
            }
        }
        
        Path destinationPath;
        if (success) {
            destinationPath = config.uploadedFolder().resolve(fileName);
            LOGGER.info("File uploaded successfully: " + fileName);
        } else if (attempts >= config.retryLimit()) {
            destinationPath = config.droppedFolder().resolve(fileName);
            LOGGER.warning("Failed to upload file after " + attempts + " attempts: " + fileName);
        } else {
            destinationPath = config.failedFolder().resolve(fileName);
            LOGGER.warning("File upload interrupted: " + fileName);
        }
        
        try {
            FileUtils.moveFileAtomically(filePath, destinationPath);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to move file: " + fileName, e);
        }
    }
}
