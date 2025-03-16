package org.example.copier;

import java.nio.file.*;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GcsUploader {
    private static final Logger logger = Logger.getLogger(GcsUploader.class.getName());
    
    public static boolean upload(final Path file, final Path gcpDir) {
        final var target = gcpDir.resolve(file.getFileName());
        try {
            Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException ex) {
            logger.log(Level.WARNING, "Failed to upload file to GCS: " + file, ex);
            return false;
        }
    }
}
