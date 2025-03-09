Write application to copy files.

Application must:
- use Java 21
- run only component specified by argument
- deserialize configuration record object from json file
- use fasterxml for json serialization and deserialization
- not use spring or any other similar framework
- use final var for all local variables, loop variables, try variables, etc. if reasonable and pragmatic
- use try-with-resources
- use switch expression
- use pattern matching
- run component specified in arguments in main function
- use key file for authorization in GCP
- use logger
- use virtual threads to wrap I/O operations
- move files atomically
- handle InterruptedException inside while loop to interrupt thread
- use while endless loop until current thread is interrupted

File Monitoring component:
- monitors folder
    - files/incoming
- starts with empty previous map of file name to file size
- lists files every one minute
- creates current map of file name to file size
- compares size from previous map with size from current map
- moves file without a change in size in two consecutive listings to files/landed
- replace previous map value by current map value

File Filtering component:
- monitors folder
    - files/landed
- uses memory mapped file for content analysis
- uses predicate to filter file
- when predicate result is true moves file to files/accepted folder
- when predicate result is false moves file to files/rejected folder
- otherwise moves file to files/failed folder

File Uploading component:
- monitors folder:
    - files/accepted
- uploads file to gcs bucket folder:
    - files/uploaded
- moves file to files/dropped folder after RETRY_LIMIT failed attempts
- after successful upload
    - create confirmation file (must use record type) in files/confirmed folder in json format with:
        - source file path (in local file system)
        - target file path (in gcs)
    - moves file to files/uploaded folder

Manifest component:
- monitors folders:
    - files/confirmed
    - files/accepted
    - files/rejected
    - files/dropped
    - files/failed
- every one hour or after reaching 1000 files list files in all monitored folders folders
    - creates manifest with uploaded target file paths in manifests/confirmed folder
    - creates manifest with uploaded source file paths in manifests/accepted folder
    - creates manifest with rejected file paths in manifests/rejected folder
    - creates manifest with dropped file paths in manifests/dropped folder
    - creates manifest with failed file paths in manifests/failed folder
    - creates uber manifest (must use record type) in manifests/landed folder with
        - uploaded target files paths manifest path
        - uploaded source files paths manifest path
        - rejected files paths manifest path
        - dropped files paths manifest path
        - failed failes paths manifest path

Manifest Uploading component:
- monitors folder:
    - manifests/landed
- for each uber manifest file
    - upload uploaded target file paths manifest file to manifests/incoming folder in gcs bucket
    - moves file to manifests/dropped folder after RETRY_LIMIT failed attempts
    - after successful upload moves uber manifest file to manifests/uploaded folder

Cleaning component:
- monitors folder:
    - manifests/uploaded
- for each uber manifest file
    - moves uploaded files from uploaded files manifest to files/completed/uploaded
    - moves rejected files from rejected files manifest to files/completed/rejected
    - moves dropped files from dropped files manifest to files/completed/dropped
    - moves failed files from failed files manifest to files/completed/failed
    - moves uploaded files manifest file to manifets/completed/uploaded folder
    - moves rejected files manifest file to manifets/completed/rejected folder
    - moves dropped files manifest file to manifets/completed/dropped folder
    - moves failed files manifest file to manifets/completed/failed folder
    - moves uber manifest to manifets/completed folder

Create each file separately for download.