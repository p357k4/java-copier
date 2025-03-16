Write application to copy files. Application must:
- use Java 24
- use records
- use switch expression
- use pattern matching with switch
- use fasterxml for json serialization and deserialization
- not use any framework
- use final var for all local variables, loop variables, try variables, etc. if reasonable and pragmatic
- use try-with-resources
- use logger
- only log exceptions and file counts
- deserialize configuration record object from json file
- create all configuration directories on startup
- use timestamps in UTC zone in ISO format
- use StructuredTaskScope to execute component function
- use StructuredTaskScope to execute move file and delete file operations
- move files atomically
- log only important facts as warning or errors
- write code using concise and simple style

Common Function Runner:
- accepts component reference as parameter
- accepts interval time as parameter
- uses while endless loop until current thread is interrupted
- sleeps for specified period
- handles InterruptedException inside while loop to interrupt thread
- provides StructuredTaskScope as a parameter for component function 
- uses StructuredTaskScope to count successful and failed tasks
- logs processed count and failed count

Incoming File Monitoring component function:
- lists files in folder and its subfolders: files/incoming
- uses immutable maps
- starts with empty previous map of file name to file size
- lists files every one minute and creates current map of file name to file size
- compares file sizes from previous map with file sizes from current map
- submits task to move each file without a change in size in two consecutive listings to folder: files/landed
- assigns current map variable to previous map variable

File Decompress component function:
- lists files in folder and its subfolders: files/compressed
- decompress zip archive preserving directory structure to folder: files/incoming
- after decompressiing zip archive moves file to folder: files/completed

File Filtering component function:
- lists files in folder and its subfolders: files/landed
- moves files with zip extension to folder: files/compressed
- uses memory mapped file for content analysis
- uses predicate to filter file
- when predicate result is true moves file to folder: files/accepted
- when predicate result is false moves file to folder: files/rejected
- in case of error or exception moves file to folder: files/failed

File Uploading component function:
- lists files in folder and its subfolders: files/accepted
- when file is older than an hour moves file to folder files/dropped
- uploads file to gcs bucket folder: files/uploaded
- after successful upload moves file to folder: files/uploaded
- in case of error or exception moves file to folder: files/failed

Manifest Creator component function:
- lists files in folder: manifests/incoming
- for each manifest file in folder: manifest/incoming move each file from manifest to appropriate relative folder in folder: files/completed then move manifest to folder: manifests/landed
- lists files in folders and its subfolders: files/uploaded, files/rejected, files/dropped, files/failed
- creates manifest in folder: manifests/incoming after every one hour or after reaching 10000 files in all monitored folders 
- manifest must include list of file paths and file sizes

Manifest Uploading component function:
- lists files in folder and its subfolders: manifest/landed
- when file is older than an hour moves manifest file to folder: manifests/dropped
- creates temporary manifest in memory by filtering uploaded file paths from original manifest
- uploads temporary manifest file to gcs folder: manifests/incoming
- after successful upload moves manifest file to folder: manifests/uploaded
- in case of error or exception moves manifest file to folder: manifests/failed

Manifest Registering component function:
- lists files in folder and its subfolders: manifest/uploaded
- register in database name of manifest file, count of uploaded files, count of failed files, count of dropped files, count of rejected files
- after successful upload moves manifest file to folder: manifests/registered
- in case of error or exception moves manifest file to folder: manifests/failed

Cleaning component function:
- lists manifest files in folder and its subfolders: manifests/registered
- moves manifest files to folder: manifests/completed

Create classes in separate files.
Emulate upload to GCS as copy operation to folder gcp
