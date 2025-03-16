package org.example.copier;

// ComponentRunner.java
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class ComponentRunner implements Runnable {
    private final Callable<FileStats> component;
    private final long sleepMillis;
    private static final Logger logger = Logger.getLogger(ComponentRunner.class.getName());

    public ComponentRunner(final Callable<FileStats> component, final long sleepMillis) {
        this.component = component;
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final var stats = component.call();
                logger.warning("Processed files: " + stats.processed() + ", Failed files: " + stats.failed());
                Thread.sleep(sleepMillis);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("ComponentRunner interrupted.");
            } catch (final Exception e) {
                logger.severe("Exception in component: " + e.getMessage());
            }
        }
    }
}
