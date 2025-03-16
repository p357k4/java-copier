Write application to copy files. Application must:
- use Java 21
- use records
- use switch expression
- use pattern matching with switch
- use fasterxml for json serialization and deserialization
- not use any framework
- use final var for all local variables, loop variables, try variables, etc. if reasonable and pragmatic
- use try-with-resources
- use logger to log important events
- move files atomically
- deserialize configuration record object from json file
- create all directories in configuration on startup
- use timestamps in UTC zone in ISO format
- implement callable interface in components to return statistics about processed and failed files
- use StructuredTaskScope to execute component functions wrapped in component runner
- log only important facts as warning or errors
- write code in concise form

Common Component Runner:
- accepts component reference as parameter
- accepts sleep time as parameter
- uses while endless loop until current thread is interrupted
- sleeps for specified period
- handles InterruptedException inside while loop to interrupt thread
- logs processed files count and failed files count

Incoming File Monitoring component function:
- lists files in folder and its subfolders: files/incoming
- uses StructuredTaskScope
- uses immutable maps
- starts with empty previous map of file name to file size
- lists files every one minute and creates current map of file name to file size
- compares file sizes from previous map with file sizes from current map
- submits task to move each file without a change in size in two consecutive listings to folder: files/landed
- assigns current map variable to previous map variable

File Filtering component function:
- lists files in folder and its subfolders: files/landed
- uses StructuredTaskScope
- submits processing of each file
- uses memory mapped file for content analysis
- uses predicate to filter file
- when predicate result is true moves file to folder: files/accepted
- when predicate result is false moves file to folder: files/rejected
- in case of error or exception moves file to folder: files/failed

File Uploading component function:
- lists files in folder and its subfolders: files/accepted
- uses StructuredTaskScope
- submits processing of each file
- when file is older than an hour moves file to folder files/dropped
- uploads file to gcs bucket folder: files/uploaded
- after successful upload moves file to folder: files/uploaded
- in case of error or exception moves file to folder: files/failed

Manifest component function:
- lists files in folders and its subfolders: files/uploaded, files/rejected, files/dropped, files/failed
- lists files in folder: manifests/incoming
- uses StructuredTaskScope
- submits processing of each file
- for each manifest file in folder: manifest/incoming move each file from manifest to appropriate relative folder in folder: files/completed then move manifest to folder: manifests/landed
- every one hour or after reaching 10000 files list files in all monitored folders and creates manifest in folder: manifests/incoming
- manifest must include list of files paths and file sizes

Manifest Uploading component function:
- lists files in folder and its subfolders: manifest/landed
- uses StructuredTaskScope
- submits processing of each file
- when file is older than an hour moves manifest file to folder: manifests/dropped
- creates temporary manifest in memory by filtering uploaded file paths from original manifest
- uploads temporary manifest file to gcs folder: manifests/incoming
- after successful upload moves manifest file to folder: manifests/uploaded
- in case of error or exception moves manifest file to folder: manifests/failed

Manifest Registrating component function:
- lists files in folder and its subfolders: manifest/uploaded
- uses StructuredTaskScope
- submits processing of each file
- register in database name of manifest file, count of uploaded files, count of failed files, count of dropped files, count of rejected files
- after successful upload moves manifest file to folder: manifests/registered
- in case of error or exception moves manifest file to folder: manifests/failed


Cleaning component function:
- lists files in folder and its subfolders: manifests/registered
- uses StructuredTaskScope
- submits processing of each processed file
- moves manifest file to folder: manifests/completed

Create classes separate files.
Emulate upload to GCS as copy operation to folder gcp
