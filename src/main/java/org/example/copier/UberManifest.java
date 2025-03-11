package org.example.copier;

import java.nio.file.Path;

// Record type representing the uber manifest.
public record UberManifest(Path uploadedManifest, Path rejectedManifest, Path droppedManifest, Path failedManifest) {}
