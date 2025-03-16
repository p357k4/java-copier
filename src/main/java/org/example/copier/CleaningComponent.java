package org.example.copier;

import java.nio.file.*;
import java.util.concurrent.StructuredTaskScope;
import java.util.logging.Logger;

public class CleaningComponent implements ComponentFunction {
    private static final Logger logger = Logger.getLogger(CleaningComponent.class.getName());

    private final Path manifestsRegistered;
    private final Path manifestsCompleted;

    public CleaningComponent(Configuration config) {
        this.manifestsRegistered = Paths.get(config.manifestsRegisteredDir());
        this.manifestsCompleted = Paths.get(config.manifestsCompletedDir());
    }

    @Override
    public void run(final StructuredTaskScope<?> scope) throws Exception {
        try (final var paths = Files.walk(manifestsRegistered)) {
            paths.filter(Files::isRegularFile).forEach(path -> scope.fork(() -> {
                final var relative = manifestsRegistered.relativize(path);
                FileOperations.moveFileAtomically(path, manifestsCompleted.resolve(relative));
                return null;
            }));
        }
    }
}
