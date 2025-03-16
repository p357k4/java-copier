package org.example.copier;
// FileProcessingApplication.java
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class FileProcessingApplication {
    private static final Logger logger = Logger.getLogger(FileProcessingApplication.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public record Config(
        String incomingDir,
        String landedDir,
        String acceptedDir,
        String rejectedDir,
        String failedDir,
        String uploadedDir,
        String droppedDir,
        String completedDir,
        String manifestIncomingDir,
        String manifestLandedDir,
        String manifestUploadedDir,
        String manifestDroppedDir,
        String manifestFailedDir,
        String manifestRegisteredDir,
        String manifestCompletedDir,
        String gcpDir,
        long fileMonitorIntervalMillis,
        long componentSleepMillis
    ) {}
    
    public static void main(final String[] args) {
        try {
            final var configPath = Paths.get("config.json");
            final var config = mapper.readValue(configPath.toFile(), Config.class);
            
            // Create all required directories
            final var dirs = new String[] {
                config.incomingDir(), config.landedDir(), config.acceptedDir(), config.rejectedDir(),
                config.failedDir(), config.uploadedDir(), config.droppedDir(), config.completedDir(),
                config.manifestIncomingDir(), config.manifestLandedDir(), config.manifestUploadedDir(),
                config.manifestDroppedDir(), config.manifestFailedDir(), config.manifestRegisteredDir(),
                config.manifestCompletedDir(), config.gcpDir()
            };
            for (final var dir : dirs) {
                Files.createDirectories(Path.of(dir));
            }
            
            final var executor = Executors.newCachedThreadPool();
            
            // Create component runners with configured sleep intervals
            final var incomingRunner = new ComponentRunner(
                new IncomingFileMonitoringComponent(config.incomingDir(), config.landedDir()), config.fileMonitorIntervalMillis());
            final var filteringRunner = new ComponentRunner(
                new FileFilteringComponent(config.landedDir(), config.acceptedDir(), config.rejectedDir(), config.failedDir()), config.componentSleepMillis());
            final var uploadingRunner = new ComponentRunner(
                new FileUploadingComponent(config.acceptedDir(), config.uploadedDir(), config.droppedDir(), config.failedDir(), config.gcpDir()), config.componentSleepMillis());
            final var manifestRunner = new ComponentRunner(
                new ManifestComponent(config.uploadedDir(), config.rejectedDir(), config.droppedDir(), config.failedDir(),
                        config.completedDir(), config.manifestIncomingDir(), config.manifestLandedDir()), config.componentSleepMillis());
            final var manifestUploadingRunner = new ComponentRunner(
                new ManifestUploadingComponent(config.manifestLandedDir(), config.manifestDroppedDir(),
                        config.manifestUploadedDir(), config.manifestFailedDir(), config.gcpDir()), config.componentSleepMillis());
            final var manifestRegistratingRunner = new ComponentRunner(
                new ManifestRegistratingComponent(config.manifestUploadedDir(), config.manifestRegisteredDir(), config.manifestFailedDir()), config.componentSleepMillis());
            final var cleaningRunner = new ComponentRunner(
                new CleaningComponent(config.manifestRegisteredDir(), config.manifestCompletedDir()), config.componentSleepMillis());
            
            // Submit all runners to the executor
            executor.submit(incomingRunner);
            executor.submit(filteringRunner);
            executor.submit(uploadingRunner);
            executor.submit(manifestRunner);
            executor.submit(manifestUploadingRunner);
            executor.submit(manifestRegistratingRunner);
            executor.submit(cleaningRunner);
            
            logger.warning("File processing application started.");
        } catch (final Exception e) {
            logger.severe("Application failed to start: " + e.getMessage());
        }
    }
}
