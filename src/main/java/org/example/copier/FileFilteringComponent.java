package org.example.copier;

import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileFilteringComponent implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(FileFilteringComponent.class.getName());

    private final Path failedDir;
    private final Path rejectedDir;
    private final Path acceptedDir;
    private final Path landedDir;

    public FileFilteringComponent(Configuration config) {
        this.landedDir = Paths.get(config.filesLandedDir());
        this.acceptedDir = Paths.get(config.filesAcceptedDir());
        this.rejectedDir = Paths.get(config.filesRejectedDir());
        this.failedDir = Paths.get(config.filesFailedDir());
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        try (final var paths = Files.walk(landedDir)) {
            paths.filter(Files::isRegularFile).forEach(path -> scope.fork(() -> {
                // Use memory-mapped file for content analysis.
                try (final var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                    final var buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
                    final Predicate<MappedByteBuffer> predicate = _ -> true;
                    final var target = predicate.test(buffer) ? acceptedDir : rejectedDir;
                    final var relative = landedDir.relativize(path);
                    FileOperations.moveFileAtomically(path, target.resolve(relative));
                } catch (Exception ex) {
                    logger.log(Level.WARNING, "Error filtering file: " + path, ex);
                    final var relative = landedDir.relativize(path);
                    FileOperations.moveFileAtomically(path, failedDir.resolve(relative));
                }
                return null;
            }));
        }
    }
}
