package org.example.copier;

import java.nio.file.*;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileOperations {
    private static final Logger logger = Logger.getLogger(FileOperations.class.getName());

    public static void moveFileAtomically(final Path source, final Path target) {
        try {
            Files.createDirectories(target.getParent());
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            logger.log(Level.WARNING, "Failed to move file: " + source, ex);
        }
    }
}
