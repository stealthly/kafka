Test scripts for KIP-4
======================

You can find test scripts for triggering topic 1) creation 2) alteration or 3) deletion in kafka/bin:
 
 test-create-topic.sh, test-alter-topic.sh, test-delete-topic.sh
   
Scripts are started as the following:

    # bin/test-create-topic.sh <path-to-file-with-commands> <broker-ips>
    
where 'file with commands' is the file containing topic commands of one type (create, delete or alter) - 
one per line. This is to test separately Create-, Alter- and DeleteTopicRequest which are **batch** requests.
Test class will parse all commands from one file and gather those to one request, send it to the server and print
the response.

Example:

    # bin/test-create-topic.sh create.txt 192.168.86.20:9092,192.168.86.25:9092,192.168.86.30:9092

    
Example of test file (contents) for create-topic:
    
    --topic t1 --partitions 5 --replication-factor 3 --config segment.bytes=2555000
    --topic t2 --partitions 5 --replication-factor 5
    --topic t3 --replica-assignment 1:2,2:3,1:3,2:3
    --topic t4 --replica-assignment 1:2:3,3:2:1,2:3:1,1:2:3
    --topic t5 --replica-assignment 1,2,3
    
    
Example of test file (contents) for alter-topic:
        
    --topic t1 --partitions 8
    --topic t2 --replication-factor 2
    --topic t3 --partitions 4
    --topic t4 --replica-assignment 2:1:3,1:2:3
    --topic t5 --replica-assignment 1,2,3
    
Example of test file (contents) for delete-topic:
        
    --topic t2
    --topic t5

    
So to test KIP-4 Phase 1 you need:

1. Build kafka from this branch to get KIP-4 changes

2. Deploy kafka from built tar somewhere

3. Prepare test files for create, alter, delete or take the examples above

4. Use scripts test-*-topic.sh under kafka/bin