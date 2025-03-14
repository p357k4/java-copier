package org.example.copier;

// FileUtils.java
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;
import java.util.logging.Level;

public class FileUtils {
    private static final Logger LOGGER = Logger.getLogger(FileUtils.class.getName());
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    
    /**
     * Moves a file atomically from source to destination.
     * 
     * @param source The source file path
     * @param target The target file path
     * @throws IOException If an I/O error occurs
     */
    public static void moveFileAtomically(Path source, Path target) throws IOException {
        LOGGER.info("Moving file atomically from " + source + " to " + target);
        
        // Ensure the parent directory exists
        Files.createDirectories(target.getParent());
        
        // Move the file atomically
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
    
    /**
     * Gets the current timestamp in UTC zone in ISO format.
     * 
     * @return The current timestamp string
     */
    public static String getCurrentTimestampUtc() {
        final var now = Instant.now();
        return ISO_FORMATTER.format(now);
    }
    
    /**
     * Safely handles InterruptedException by interrupting the current thread.
     * 
     * @param e The InterruptedException
     */
    public static void handleInterruptedException(InterruptedException e) {
        LOGGER.log(Level.INFO, "Thread interrupted", e);
        Thread.currentThread().interrupt();
    }
}
