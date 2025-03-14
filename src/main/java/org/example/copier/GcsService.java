package org.example.copier;

// GcsService.java
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;
import java.util.logging.Level;

public class GcsService {
    private static final Logger LOGGER = Logger.getLogger(GcsService.class.getName());
    
    private final String bucketName;
    private final Storage storage;
    
    public GcsService(String bucketName, String keyFilePath) throws IOException {
        this.bucketName = bucketName;
        
        // Initialize GCS client with service account credentials
        try (final var keyFileStream = new FileInputStream(keyFilePath)) {
            final var credentials = ServiceAccountCredentials.fromStream(keyFileStream);
            this.storage = StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();
        }
        
        LOGGER.info("GCS Service initialized with bucket: " + bucketName);
    }
    
    /**
     * Uploads a file to Google Cloud Storage.
     * 
     * @param filePath The path to the file to upload
     * @param destinationPath The destination path in GCS
     * @return true if the upload was successful, false otherwise
     */
    public boolean uploadFile(Path filePath, String destinationPath) {
        try {
            final var blobId = BlobId.of(bucketName, destinationPath);
            final var blobInfo = BlobInfo.newBuilder(blobId)
                    .setContentType(Files.probeContentType(filePath))
                    .build();
            
            LOGGER.info("Uploading file: " + filePath + " to GCS path: " + destinationPath);
            
            try (final var fileInputStream = Files.newInputStream(filePath)) {
                storage.create(blobInfo, Files.readAllBytes(filePath));
            }
            
            LOGGER.info("File uploaded successfully: " + filePath);
            return true;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to upload file: " + filePath, e);
            return false;
        }
    }
}
