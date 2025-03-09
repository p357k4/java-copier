package org.example.copier;

// File: UberManifest.java
public record UberManifest(
    String confirmedManifestPath,
    String acceptedManifestPath,
    String rejectedManifestPath,
    String droppedManifestPath,
    String failedManifestPath
) {}
