Write application to copy files.

Application must:
- use Java 21
- run only component specified by argument
- use configuration record object
- deserialize configuration record object from json file
- not use spring or any similar framework
- use final var for all local variables, loop variables, try variables, etc.
- use try-with-resources
- run component specified in arguments for main function
- use key file for authorization in GCP
- use logger
- use virtual threads wrapping I/O operations
- move files atomically
- handle InterruptedException inside while loop to interrupt thread
- use while endless loop until current thread is interrupted

File Monitoring component:
- monitors periodically content of files/incoming folder
- moves file without a change in size in two consecutive listings to files/landed

File Filtering component:
- monitors files in files/landed folder
- uses memory mapped file for content analysis
- uses predicate to filter file
- when predicate result is positive moves file to files/accepted folder
- when predicate result is negative moves file to files/rejected folder
- otherwise moves file to files/failed folder

File Uploading component:
- monitors files in files/accepted folder
- upload file to files/uploaded in gcs bucket
- on failure use expenential backoff to repeat upload
- after RETRY_LIMIT tries move file to files/dropped folder
- after successful upload
    - create confirmation file in files/confirmed folder in json format with:
        - source file path (in local file system)
        - target file path (in gcs)
    - moves file to files/uploaded folder

Manifest component:
- monitors files/confirmed
- every one hour or after reaching 1000 files list files in folders:
    - files/confirmed
    - files/accepted
    - files/rejected
    - files/dopped
    - files/failed
- creates manifest with uploaded target file paths in manifests/confirmed folder
- creates manifest with uploaded source file paths in manifests/accepted folder
- creates manifest with rejected file paths in manifests/rejected folder
- creates manifest with dropped file paths in manifests/dropped folder
- creates manifest with failed file paths in manifests/failed folder
- creates uber manifest in manifests/landed folder with
    - path to uploaded target files paths
    - path to uploaded source files manifest
    - path to rejected files manifest
    - path to dropped files manifest
    - path to failed failes manifest 

Manifest Uploading component:
- monitors manifests/landed folder
- for uber manifest file
    - uploads uploaded target file paths manifest file to manifests/incoming folder in gcs bucket
    - after successful upload moves uber manifest to manifests/uploaded folder
    - on failure use expenential backoff to repeat upload
    - after RETRY_LIMIT tries move file to manifests/dropped folder

Cleaning component:
- monitors manifests/uploaded folder
- for uber manifest file
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