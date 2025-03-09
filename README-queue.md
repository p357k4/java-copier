Write event driven application to copy files.

Application must:
- use Java 21
- run only component specified by argument
- use configuration record object
- deserialize configuration record object from yaml file
- not use spring or any similar framework
- use sealed interfaces and records to model events
- use switch expressions
- use pattern matching
- use pattern matching in switch expressions
- use final var for all local variables, loop variables, try variables, etc.
- use try-with-resources
- use Chronicle Queue as queue implementation
- ackowledge message from queue only if processed correctly
- run component specified in arguments for main function

File Monitoring component:
- monitors periodically content of files/landing folder
- moves file without a change in size in two consecutive listings to files/landed
- publishes event FILE_LANDED with file path to queue QUEUE_FILE_LANDED

File Filtering component:
- reads event from queue QUEUE_FILE_LANDED
    - for FILE_LANDED
        - uses memory mapped file for content analysis
        - when content do matche filter publishes event FILE_ACCEPTED with file path and retry counter RETRY_LEFT to queue QUEUE_FILE_FILTERED
        - when content does not matche filter publishes event FILE_REJECTED with file to queue QUEUE_FILE_FILTERED
        - otherwise publishes event FILE_FAILED with file to queue QUEUE_FILE_FILTERED

File Uploading component:
- reads event from queue QUEUE_FILE_FILTERED
    - for FILE_ACCEPTED event
        - upload file to files/uploaded in gcs bucket
        - after successful upload publishes event FILE_UPLOADED with path of source file and with path of target file to QUEUE_FILE_PROCESSED
        - after failed upload publishes event FILE_DEFERRED with path of source file to queue QUEUE_FILE_DEFERRED
    - for FILE_REJECTED event
        - republishes event to queue QUEUE_FILE_PROCESSED
    - for FILE_FAILED event
        - republishes event to queue QUEUE_FILE_PROCESSED
- reads event from queue QUEUE_FILE_DEFERRED
    - for FILE_DEFERRED
        - when retry counter is zero publishes event FILE_FAILED with path from input event to queue QUEUE_FILE_PROCESSED
        - otherwise publishes event FILE_DEFERRED with path from input event to QUEUE_FILE_DEFERRED and retry counter decreased by one

Manifest component:
- reads event from queue QUEUE_FILE_PROCESSED
    - for FILE_UPLOADED event
        - appends path to uploaded files manifest file
        - ensures change in uploaded files manifest file is durable
    - for FILE_REJECTED event
        - appends path to rejected files manifest file
        - ensures change in rejected files manifest file is durable
    - for FILE_FAILED event
        - appends path to failed files manifest file
        - ensures change in failed files manifest file is durable
    - completes manifests file every one hour and after reaching 1000 entries
    - publishes event MANIFEST_ACCEPTED with path to uploaded files manifest file and with path to rejected files manifest file and with path to failed files manifest file to queue QUEUE_MANIFEST_ACCEPTED and retry counter RETRY_LEFT

Manifest Uploading component:
- reads event from queue QUEUE_MANIFEST_ACCEPTED
    - for MANIFEST_ACCEPTED event
        - uploads uploaded files manifest file manifests/uploaded folder in gcs bucket
        - after successful upload publishes event MANIFEST_PROCESSED to queue QUEUE_MANIFEST_PROCESSED with paths from input event
        - after failed upload publishes event MANIFEST_DEFERRED with paths from input event to queue QUEUE_MANIFEST_DEFERRED
- reads event from queue QUEUE_MANIFEST_DEFERRED
    - for MANIFEST_DEFERRED
        - when retry counter is zero publishes event MANIFEST_FAILED with path from input event to queue QUEUE_MANIFEST_PROCESSED
        - otherwise after 5 seconds publishes event MANIFEST_DEFERRED with path from input event to QUEUE_MANIFEST_DEFERRED and retry counter decreased by 1

Cleaning component:
- reads event from queue QUEUE_MANIFEST_PROCESSED
- for MANIFEST_PROCESSED event
    - move all files from uploaded files manifest file to files/uploaded
    - move all files from rejected files manifest file to files/rejected
    - move all files from failed files manifest file to files/failed
    - moves manifests file to manifests/completed
    - log summary with info level
- for MANIFEST_FAILED event
    - move all files from uploaded files manifest file to files/uploaded
    - move all files from rejected files manifest file to files/rejected
    - move all files from failed files manifest file to files/failed
    - moves manifest file to manifests/failed
    - log summary with error level

Create each file separately for download.