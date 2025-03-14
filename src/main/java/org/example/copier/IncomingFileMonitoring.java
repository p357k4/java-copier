package org.example.copier;

// IncomingFileMonitoring.java
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class IncomingFileMonitoring {
    private static final Logger LOGGER = Logger.getLogger(IncomingFileMonitoring.class.getName());
    private static final long SCAN_INTERVAL_MINUTES = 1;
    
    private final Configuration config;
    private Map<String, Long> previousFileMap;
    
    public IncomingFileMonitoring(Configuration config) {
        this.config = config;
        this.previousFileMap = new HashMap<>();
        LOGGER.info("Incoming File Monitoring initialized to watch folder: " + config.incomingFolder());
    }
    
    public void run() {
        LOGGER.info("Incoming File Monitoring started");
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                processIncomingFiles();
                TimeUnit.MINUTES.sleep(SCAN_INTERVAL_MINUTES);
            } catch (InterruptedException e) {
                FileUtils.handleInterruptedException(e);
                break;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing incoming files", e);
            }
        }
        
        LOGGER.info("Incoming File Monitoring stopped");
    }
    
    private void processIncomingFiles() throws IOException {
        LOGGER.info("Scanning incoming folder: " + config.incomingFolder());
        
        // Get current file map
        final var currentFileMap = Files.list(config.incomingFolder())
                .filter(Files::isRegularFile)
                .collect(Collectors.toMap(
                        path -> path.getFileName().toString(),
                        path -> {
                            try {
                                return Files.size(path);
                            } catch (IOException e) {
                                LOGGER.log(Level.WARNING, "Failed to get file size: " + path, e);
                                return -1L;
                            }
                        }
                ));
        
        LOGGER.info("Found " + currentFileMap.size() + " files in incoming folder");
        
        // Compare file sizes
        for (final var entry : currentFileMap.entrySet()) {
            final var fileName = entry.getKey();
            final var currentSize = entry.getValue();
            
            if (previousFileMap.containsKey(fileName)) {
                final var previousSize = previousFileMap.get(fileName);
                
                if (currentSize.equals(previousSize) && currentSize > 0) {
                    // File size hasn't changed, move to landed folder
                    final var sourcePath = config.incomingFolder().resolve(fileName);
                    final var targetPath = config.landedFolder().resolve(fileName);
                    
                    try {
                        FileUtils.moveFileAtomically(sourcePath, targetPath);
                        LOGGER.info("Moved file to landed folder: " + fileName);
                    } catch (IOException e) {
                        LOGGER.log(Level.SEVERE, "Failed to move file: " + fileName, e);
                    }
                }
            }
        }
        
        // Update previous file map
        previousFileMap = currentFileMap;
    }
}
