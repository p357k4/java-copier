package org.example.copier;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class IncomingFileMonitoring implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(IncomingFileMonitoring.class.getName());
    private Map<Path, Long> previous = Map.of(); // empty immutable map

    private final Path incomingDir;
    private final Path landedDir;
    private final Path compressedDir;

    public IncomingFileMonitoring(Configuration config) {
        this.incomingDir = Paths.get(config.filesIncomingDir());
        this.landedDir = Paths.get(config.filesLandedDir());
        this.compressedDir = Paths.get(config.filesCompressedDir());
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        try (final var paths = Files.walk(incomingDir)) {
            final var current = paths.filter(Files::isRegularFile).collect(Collectors.toMap(path -> path, t -> {
                try {
                    return Files.size(t);
                } catch (IOException e) {
                    return -1L;
                }
            }));

            current.forEach((path, size) -> {
                final var previousSize = previous.get(path);
                if (previousSize == null) {
                    return;
                }

                if (!previousSize.equals(size)) {
                    return;
                }

                final var dir = path.toString().endsWith("zip") ? compressedDir : landedDir;

                // File size unchanged—move the file.
                final var relative = incomingDir.relativize(path);
                final var target = dir.resolve(relative);
                scope.fork(() -> {
                    FileOperations.moveFileAtomically(path, target);
                    return null;
                });
            });

            previous = current;
        }
    }
}
