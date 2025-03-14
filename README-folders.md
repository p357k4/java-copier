Write application in Java.

Must:
- use Java 21
- run component specified in arguments in main function
- use fasterxml for json serialization and deserialization
- not use any framework
- use final var for all local variables, loop variables, try variables, etc. if reasonable and pragmatic
- use try-with-resources
- use key file for authorization in GCP
- use logger
- use virtual threads for blocking operations
- move files atomically
- handle InterruptedException inside while loop to interrupt thread
- use while endless loop until current thread is interrupted
- deserialize configuration record object from json file
- create all directories in configuration
- use timestamps in UTC zone in ISO format

Incoming File Monitoring component:
- monitors files in folder: files/incoming
- starts with empty previous map of file name to file size
- lists files every one minute and creates current map of file name to file size
- compares file sizes from previous map with file sizes from current map
- moves file without a change in size in two consecutive listings to folder: files/landed
- assigns current map variable to previous map variable

File Filtering component:
- monitors files in folder: files/landed
- uses memory mapped file for content analysis
- uses predicate to filter file
- when predicate result is true moves file to folder: files/accepted
- when predicate result is false moves file to folder: files/rejected
- otherwise moves file to folder: files/failed

File Uploading component:
- monitors files in folder: files/accepted
- uploads file to gcs bucket folder: files/uploaded
- moves file to files/dropped folder after RETRY_LIMIT failed attempts
- after successful upload moves file to folder: files/uploaded
- otherwise moves file to folder: files/failed

Manifest component:
- monitors files in folders: files/*
- every one hour or after reaching 1000 files list files in all monitored folders folders and creates manifest with list of files paths in folder: manifests/landed

Manifest Uploading component:
- monitors folder: manifest/landed
- creates uploaded files manifest file with uploaded files path from manifest file
- uploads uploaded files manifest file to gcs folder: manifests/incoming
- after RETRY_LIMIT failed attempts moves manifest file to folder: manifests/dropped
- after successful upload
    - moves manifest file to folder: manifests/uploaded
- otherwise moves manifest file to folder: manifests/failed

Cleaning component:
- monitors folder: manifests/uploaded
- moves all files from manifest to appropriate relative folder in folder files/completed
- moves manifest file to folder: manifests/completed

Create classes separately.