package org.example.copier;

import java.util.concurrent.StructuredTaskScope;

@FunctionalInterface
public interface ComponentFunction {
    void run(StructuredTaskScope<?> scope) throws Exception;
}
