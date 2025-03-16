package org.example.copier;

import java.nio.file.*;
import java.io.*;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

public class FileDecompressComponent implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(FileDecompressComponent.class.getName());

    private final Path completedDir;
    private final Path incomingDir;
    private final Path compressedDir;

    public FileDecompressComponent(Configuration config) {
        this.compressedDir = Paths.get(config.filesCompressedDir());
        this.incomingDir = Paths.get(config.filesIncomingDir());
        this.completedDir = Paths.get(config.filesCompletedDir());
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        try (final var paths = Files.walk(compressedDir)) {
            paths.filter(Files::isRegularFile).filter(path -> path.toString().endsWith(".zip")).forEach(path -> {
                try (final var z = new ZipFile(path.toFile())) {
                    final var entries = z.entries();
                    while (entries.hasMoreElements()) {
                        final var entry = entries.nextElement();
                        if (entry == null) {
                            break;
                        }

                        final var uncompressed = incomingDir.resolve(entry.getName());
                        if (entry.isDirectory()) {
                            continue;
                        }

                        try (final var is = z.getInputStream(entry)) {
                            final var bytes = is.readAllBytes();
                            scope.fork(() -> {
                                Files.createDirectories(uncompressed.getParent());
                                Files.write(uncompressed, bytes);
                                return null;
                            });
                        }
                    }
                    scope.join();
                    final var relative = compressedDir.relativize(path);
                    FileOperations.moveFileAtomically(path, completedDir.resolve(relative));
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Error decompressing file: " + path, ex);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    logger.log(Level.WARNING, "Interrupted file decompression: " + path, ex);
                }
            });
        }
    }
}
