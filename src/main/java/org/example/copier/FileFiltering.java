package org.example.copier;

// FileFiltering.java
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.logging.Level;

public class FileFiltering {
    private static final Logger LOGGER = Logger.getLogger(FileFiltering.class.getName());
    private static final long SCAN_INTERVAL_SECONDS = 10;
    
    private final Configuration config;
    private final Predicate<MappedByteBuffer> filePredicateFilter;
    
    public FileFiltering(Configuration config) {
        this.config = config;
        this.filePredicateFilter = createFileFilter();
        LOGGER.info("File Filtering initialized to watch folder: " + config.landedFolder());
    }
    
    private Predicate<MappedByteBuffer> createFileFilter() {
        // Example implementation of a file filter
        // This would be replaced with a concrete implementation based on requirements
        return buffer -> {
            try {
                // Example: Check if the file contains a specific byte sequence
                byte[] header = new byte[4];
                buffer.get(header);
                buffer.rewind();
                
                // Example: Check if file starts with "FILE"
                return header[0] == 'F' && header[1] == 'I' && header[2] == 'L' && header[3] == 'E';
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error filtering file", e);
                return false;
            }
        };
    }
    
    public void run() {
        LOGGER.info("File Filtering started");
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                processLandedFiles();
                TimeUnit.SECONDS.sleep(SCAN_INTERVAL_SECONDS);
            } catch (InterruptedException e) {
                FileUtils.handleInterruptedException(e);
                break;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing landed files", e);
            }
        }
        
        LOGGER.info("File Filtering stopped");
    }
    
    private void processLandedFiles() throws IOException {
        LOGGER.info("Scanning landed folder: " + config.landedFolder());
        
        try (final var files = Files.list(config.landedFolder())) {
            files.filter(Files::isRegularFile).forEach(this::processFile);
        }
    }
    
    private void processFile(Path filePath) {
        final var fileName = filePath.getFileName().toString();
        LOGGER.info("Processing file: " + fileName);
        
        Path destinationPath;
        
        try (final var fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            final var fileSize = fileChannel.size();
            final var buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            
            boolean predicateResult;
            try {
                predicateResult = filePredicateFilter.test(buffer);
                
                if (predicateResult) {
                    destinationPath = config.acceptedFolder().resolve(fileName);
                    LOGGER.info("File accepted: " + fileName);
                } else {
                    destinationPath = config.rejectedFolder().resolve(fileName);
                    LOGGER.info("File rejected: " + fileName);
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error applying filter to file: " + fileName, e);
                destinationPath = config.failedFolder().resolve(fileName);
                LOGGER.info("File processing failed: " + fileName);
            }
            
            try {
                FileUtils.moveFileAtomically(filePath, destinationPath);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to move file: " + fileName, e);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to open file: " + fileName, e);
            try {
                final var failedPath = config.failedFolder().resolve(fileName);
                FileUtils.moveFileAtomically(filePath, failedPath);
            } catch (IOException moveEx) {
                LOGGER.log(Level.SEVERE, "Failed to move file to failed folder: " + fileName, moveEx);
            }
        }
    }
}
