package org.example.copier;

import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ComponentRunner {
    private static final Logger logger = Logger.getLogger(ComponentRunner.class.getName());

    public static void runComponent(final ComponentFunction component, final long interval) {
        while (!Thread.currentThread().isInterrupted()) {
            try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                component.run(scope);
                scope.join(); // wait for all tasks launched in this iteration
                TimeUnit.MILLISECONDS.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Component error", e);
            }
        }
    }
}
