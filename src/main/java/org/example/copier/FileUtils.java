package org.example.copier;

// FileUtils.java
import java.nio.file.*;
import java.time.Instant;
import java.io.IOException;

public class FileUtils {
    public static void moveFileAtomically(Path source, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    public static void copyFile(Path source, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    public static boolean isOlderThan(Path path, Instant old) throws IOException {
        final var fileTime = Files.getLastModifiedTime(path).toInstant();
        return fileTime.isBefore(old);
    }
}
