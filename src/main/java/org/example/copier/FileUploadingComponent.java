package org.example.copier;

import java.nio.file.*;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.Duration;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileUploadingComponent implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(FileUploadingComponent.class.getName());

    private final Path failedDir;
    private final Path droppedDir;
    private final Path acceptedDir;
    private final Path uploadedDir;
    private final Path gcpDir;

    public FileUploadingComponent(Configuration config) {
        this.uploadedDir = Paths.get(config.filesUploadedDir());
        this.acceptedDir = Paths.get(config.filesAcceptedDir());
        this.droppedDir = Paths.get(config.filesDroppedDir());
        this.failedDir = Paths.get(config.filesFailedDir());
        this.gcpDir = Paths.get(config.gcpDir());
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        try (final var paths = Files.walk(acceptedDir)) {
            paths.filter(Files::isRegularFile).forEach(path -> scope.fork(() -> {
                try {
                    final var lastModified = Files.getLastModifiedTime(path).toInstant();
                    final var now = Instant.now();
                    if (lastModified.plus(1, ChronoUnit.HOURS).isBefore(now)) {
                        FileOperations.moveFileAtomically(path, droppedDir.resolve(path.getFileName()));
                    } else if (GcsUploader.upload(path, gcpDir)) {
                        FileOperations.moveFileAtomically(path, uploadedDir.resolve(path.getFileName()));
                    } else {
                        FileOperations.moveFileAtomically(path, failedDir.resolve(path.getFileName()));
                    }
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Error uploading file: " + path, ex);
                    FileOperations.moveFileAtomically(path, failedDir.resolve(path.getFileName()));
                }
                return null;
            }));
        }
    }
}
