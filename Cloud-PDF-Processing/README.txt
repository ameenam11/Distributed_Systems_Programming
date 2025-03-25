Ameen Amer - *********
Raya Wattad - *********


▪ How to run our project: 

Answer: First, open a terminal and run the command "mvn clean package" to compile the code and make the jar files.
	Then, to run a local application you should use this command : "java -jar target/LocalApp.jar <input-file-name> <output-file-name> n -t(optional)". 
	*Important note: In order to run the program properly you should first add your input file into the directory "localAppInputFiles" and then put the NAME of the file in the arguments! (Again - put the NAME of the file and not its 	path!)


▪ How our program works:

Answer: 
1) The local app parses its arguments and gets an ID from the system based on the current time millis.
2) The first local app to enter creates: two buckets in S3 - one for the files and one for the jars(Manager and Workers jars) -, and two SQS queues for communicating with the Manager - appToManager and managerToApp queues -.
3) The first app uploads the jar files to S3 bucket-for-jars, and then activates the Manager.
4) The local app uploads the input file on S3 and then sends the Manager a message which contains the ID of the app(for mapping the apps to their files) and URL of the uploaded input file via the SQS appToManager and then enters a loop waiting for its Summary File. 
5) The Manager starts with creating two SQS queues for communicating with the workers - managerToWorkers and workersToManager queues -. 
6) The Manager activates two listener threads - one for listening to the apps and one for listening to the workers -, and two thread pools - one for processing messages from the apps and one for processing messages from the workers -.
7) Once a message is detected from an app, the listener thread checks if it is a terminate message. if so, it terminates. otherwise, it sends the message to the "apps" pool. 
8) The pool processes the message - parses the message, downloads the input file locally from S3 and counts all its lines and sends them line by line to the workers via the managerToWorkers SQS - and then activates the required number of workers.
9) The worker starts with receiving messages from managerToWorkers queue and processes each line - takes the PDF's URL and the operation that appear on the line, downloads the PDF file locally and then performs the operation on the file -.
10) The worker checks if the operation was done successfully on the PDF file and if so uploads the converted PDF file to S3 and sends the Manager a message of the two URLs, otherwise sends the message the the original URL and an error message.
11) The Manager(the "workers listener") gets the messages from the workers via the queue workersToManager and sends them to the "workers pool". 
12) The workers pool accumulates the messages for the specified local app(based on its ID) in a class that we created called "AppProcessingStates" which has two fields - one is the remaining lines for each app(integer), and one is a data structure that saves the lines that was sent from the workers to the specified app(concurrentLinkedQueue). 
13) If the remaining lines of an app becomes 0, the Manager(the thread in the workers pool) makes a text summary file that consist of all the lines that were accumulated and: uploads it to S3, sends a message to the local app, and removes the local app's id from the data structure appsStates. 
14) The local app gets the summary file's URL from managerToApp queue, downloads it from S3 to the directory "localAppOutputFiles", converts it to HTML file, deletes the message from the queue, and sends the manager a termination message(optional).
15) The Manager(the workers listener thread) doesn't terminate unless: all the workers have finished their work, the apps listener was terminated(i.e a terminate message was sent from the local app), and the all the local apps have taken their summary files URLs from the managerToApp queue. If all these conditions were satisfied then the Manager deletes all the resources that were allocated for the program - such as SQS queues, Workers nodes, etc...



** In order to take care of the fail that one worker ec2 node crashes or stops unexpectedly we used another thread which we called "health checker", which keeps checking the health(the state of each worker - running/stopped/terminated) of all the workers nodes every two minutes, and if one is found "unhealthy" it launches another instance. 



▪ We used two instances of AMI that WE created - one image for Manager instance, and the other for Worker instance. 

▪ We used micro_large.



▪ We ran 4 local apps with the input files that were provided with the assignment(1500 line each input file) with n = 10, and the time that took the program to finish working was 10-15 minutes. 




▪ Did you think for more than 2 minutes about security? Do not send your credentials in plain 
text!

Answer : Yes, security was considered. Credentials are never sent in plain text; instead, we store them in a nano file "~/.aws/credentials" for the local apps. Communication between nodes and with external services is done over encrypted channels (HTTPS), and pre-signed URLs are used for secure, time-limited access to files in S3.



▪ Did you think about scalability? Will your program work properly when 1 million clients 
connected at the same time? How about 2 million? 1 billion? Scalability is very important 
aspect of the system, be sure it is scalable! 

Answer : Scalability was addressed by designing the system to handle concurrent clients efficiently. For example:
	- AWS S3 and EC2 scale automatically based on demand. We have one bucket for the files and one for the jars, and for each app we make a unique folder inside the bucket and store in it the local app's files.
	- We use a message queue (e.g., SQS) for task distribution, which allows the system to process tasks asynchronously and handle spikes in workload.
	- The manager uses a thread pool to process tasks concurrently, and it is configured to adjust dynamically to accommodate a growing number of requests.
	  The architecture is modular, so adding more worker nodes or increasing resources is straightforward.



▪ What about persistence? What if a node dies? What if a node stalls for a while? Have you 
taken care of all possible outcomes in the system? Think of more possible issues that might 
arise from failures. What did you do to solve it? What about broken communications? Be 
sure to handle all fail-cases! 

Answer : We ensure persistence and handle failures by using S3 for reliable storage, SQS for dependable message delivery, and health checks to detect stalled or failed nodes. Tasks are retried or reassigned on failure, with mechanisms for retries, consistent processing, and automatic recovery from communication issues or resource deletions.



▪ Threads in your application, when is it a good idea? When is it bad? Invest time to think 
about threads in your application!

Answer : When threads are good:
	They are used to handle multiple tasks concurrently, such as processing SQS messages or handling multiple client connections, improving performance.
When threads are bad:
	Overusing threads can lead to contention and overhead. For instance, spawning too many threads can exhaust system resources. Instead, a thread pool with a fixed number of threads 	is used to limit concurrency and maintain performance.


▪ Did you run more than one client at the same time? Be sure they work properly, and finish 
properly, and your results are correct.

Answer : Yes! The system was tested with multiple clients running simultaneously to ensure correctness and performance. Tests confirmed that:
	- Clients do not interfere with each other.
	- Results are consistent and accurate even under high concurrency.


▪ Do you understand how the system works? Do a full run using pen and paper, draw the 
different parts and the communication that happens between them.

Answer : Yes! The system was designed and understood thoroughly. A full end-to-end flow was drawn using diagrams that map out:
	- The manager's interactions with clients and workers.
	- The flow of messages and task states between the components.


▪ Did you manage the termination process? Be sure all is closed once requested!

Answer : Yes. 


▪ Did you take in mind the system limitations that we are using? Be sure to use it to its fullest! 

Answer : Yes. The system was designed with limitations in mind:
	- Resource limits (e.g., thread pool size) are configured appropriately.
	- AWS service limits are considered, such as S3 and SQS throughput.

▪ Are all your workers working hard? Or some are slacking? Why?

Answer : Yes. We allocate the required number of workers for each input file(if possible) so we ensure that all workers do their best without stopping to perform tasks(processing lines). 


▪ Is your manager doing more work than he's supposed to? Have you made sure each part of 
your system has properly defined tasks? Did you mix their tasks? Don't!

Answer : The manager’s role is confined to distributing tasks and collecting results(also, replacing crashed workers in new workers), in other words it manages only the communication process. Also, it does not perform computationally expensive tasks, ensuring the system remains modular and scalable.


▪ Lastly, are you sure you understand what distributed means? Is there anything in your 
system awaiting another? 

Answer : Yes, "distributed" means that we divide the tasks among instances or nodes, with each one completing its assigned task. No instance is waiting.



