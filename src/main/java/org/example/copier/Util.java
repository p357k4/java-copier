package org.example.copier;

// File: Util.java
import java.nio.file.*;
import java.io.*;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Util {
    private static final Logger logger = Logger.getLogger(Util.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void atomicMove(Path source, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        logger.info("Moved file " + source + " to " + target);
    }
    
    public static boolean uploadToGCS(Path file, String bucketName, String targetFolder, String gcpKeyFile) {
        // Simulate GCS upload using the provided key file for authorization.
        // In a real implementation, use Google Cloud Storage client libraries.
        logger.info("Uploading file " + file + " to bucket " + bucketName +
                    " folder " + targetFolder + " using key file " + gcpKeyFile);
        // Simulate a successful upload.
        return true;
    }
    
    public static <T> T readJson(Path file, Class<T> valueType) throws IOException {
        try (final var reader = Files.newBufferedReader(file)) {
            return mapper.readValue(reader, valueType);
        }
    }
    
    public static void writeJson(Path file, Object value) throws IOException {
        Files.createDirectories(file.getParent());
        try (final var writer = Files.newBufferedWriter(file)) {
            mapper.writeValue(writer, value);
        }
    }
}
