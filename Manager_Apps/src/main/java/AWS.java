
import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesResponse;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class AWS {

    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String managerAMI = "ami-0e51198f77f319073";
    public static String workerAMI = "ami-05a3518145285d09d";

    public static String securityGroupID = "sg-082d4d2b7c25bc769";

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    public String bucketForFiles = "ameens-bucket-for-files";
    public String bucketForJars = "ameens-bucket-for-jars";

    public String appToManager = "appToManager";
    public String managerToApp = "managerToApp";
    public String managerToWorkers = "managerToWorkers";
    public String workersToManager = "workersToManager";

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    // S3
    public void createBucketIfNotExists(String bucketName) {
        System.out.println("[DEBUG] creating the bucket " + bucketName);
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println("[DEBUG] Successfully created the input bucket.");
        } catch (S3Exception e) {
            System.out.println("[DEBUG] The bucket is already created.");
        }
        System.out.println();

    }

    public boolean checkIfManagerIsActive() {
        try {
            Filter tagFilter = Filter.builder()
                    .name("tag:Name")
                    .values("Manager")
                    .build();

            // Build the request
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(tagFilter)
                    .build();

            // Describe instances
            DescribeInstancesResponse response = ec2.describeInstances(request);

            // Check if an instance is active
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    // Get and print the tags of the instance
                    for (Tag tag : instance.tags()) {
                        if (tag.value().equals("Manager")
                                && (instance.state().name().equals(InstanceStateName.RUNNING)
                                || instance.state().name().equals(InstanceStateName.PENDING))) {
                            // The Manager is active (either running or pending)
                            return true;
                        }
                    }
                }
            }
        } catch (Ec2Exception e) {
            System.err.println("Error checking instance tags: " + e.getMessage());
            e.printStackTrace();
        }

        return false;
    }

    // EC2
    public String createEC2(String script, String tagName, int numberOfInstances) {
        String ami = tagName.equals("Manager") ? managerAMI : workerAMI;
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE) //read about this
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey") //read about this
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .securityGroupIds(securityGroupID)
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        System.out.println();
        return instanceId;
    }

    public String uploadFileToS3(String bucketName, String filePath, String fileType) {
        String objectKey = fileType + Paths.get(filePath).getFileName().toString();

        try {

            if ("ameens-bucket-for-jars".equals(bucketName)) {
                System.out.println("[DEBUG] Checking if the jars were already uploaded..");
                ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .build();
                ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

                for (S3Object s3Object : listObjectsResponse.contents()) {
                    if (s3Object.key().equals(objectKey)) {
                        System.out.println("[DEBUG] File already uploaded.");
                        return "File already uploaded.";
                    }
                }
            }

            System.out.println("[DEBUG] Uploading the file " + objectKey + " to S3");
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();
            s3.putObject(putObjectRequest, Paths.get(filePath));

            String link = "https://" + bucketName + ".s3." + region1 + ".amazonaws.com/" + objectKey;
            System.out.println("[DEBUG] Successfully uploaded the input file to S3.");
            System.out.println();
            return link;

        } catch (S3Exception e) {
            System.err.println("[ERROR] Failed to upload file to S3: " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            System.err.println("[ERROR] Unexpected error: " + e.getMessage());
        }

        return "Upload failed.";
    }

    public void downloadFileFromS3(String bucketName, String s3Url, String localFilePath) {
        URI uri = URI.create(s3Url);
        //String bucketName = uri.getHost().split("\\.")[0];  // S3 bucket name is before the first dot
        String objectKey = uri.getPath().substring(1);  // Remove leading '/' from the key

        System.out.println("[DEBUG] downloading file " + objectKey + " from S3");
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
        s3.getObject(getObjectRequest, Paths.get(localFilePath));

        System.out.println("[DEBUG] Succesfully downloaded the summary file from S3.");
        System.out.println();
    }

    public String createSqsQueue(String queueName) {
        try {
            System.out.println("[DEBUG] Checking if SQS queue already exists: " + queueName);

            // Attempt to get the queue URL
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
            System.out.println("[DEBUG] SQS already created: " + queueUrl);
            System.out.println();
            return queueUrl;

        } catch (QueueDoesNotExistException e) {
            System.out.println("[DEBUG] Queue does not exist. Creating SQS queue: " + queueName);

            // Create the queue
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            sqs.createQueue(createQueueRequest);
            System.out.println("[DEBUG] Successfully created the SQS queue.");

            // Retrieve and return the queue URL
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            System.out.println();
            return sqs.getQueueUrl(getQueueRequest).queueUrl();
        }
    }

    public void sendMessage(String queueName, String message) {
        if (!queueName.equals(managerToWorkers) && !queueName.equals(workersToManager)) {
            System.out.println("[DEBUG] Sending message to " + queueName + " queue");
        }
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(2)
                .build();
        sqs.sendMessage(send_msg_request);

        if (!queueName.equals(managerToWorkers) && !queueName.equals(workersToManager)) {
            System.out.println("[DEBUG] Succesfully sent the message.");
            System.out.println();
        }
    }

    public List<Message> getMessagesFromSqs(String queueName, Integer visibilityTimeout) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String sqsURL = sqs.getQueueUrl(getQueueRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(sqsURL)
                .maxNumberOfMessages(10) // so the API calls be cost-efficient
                .visibilityTimeout(visibilityTimeout) // to let the workers process the message
                .build();
        return sqs.receiveMessage(receiveRequest).messages();
    }

    public String getData(String className) {
        return "#!/bin/bash\n"
                + "echo " + className + " EC2 running\n"
                + "aws s3 cp s3://" + bucketForJars + "/" + className + ".jar" + " ./" + className + "Files/" + className + ".jar\n"
                + "echo " + className + " copied the jar from s3\n"
                + "java -jar /" + className + "Files/" + className + ".jar\n";

    }

    public int getApproximateNumberOfMessages(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build();
            GetQueueAttributesResponse response = sqs.getQueueAttributes(request);

            String messageCount = response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);

            return Integer.parseInt(messageCount);
        } catch (Exception e) {
            System.out.println("[DEBUG] Error getting the number of messages in queue: " + queueName);
            return -1;
        }
    }

    public String generatePresignedUrl(String bucketName, String key) {
        // Define the region where your S3 bucket is located
        Region bucketRegion = Region.US_WEST_2; // Adjust this to your bucket's region

        // Initialize the S3Presigner with the correct region
        try (S3Presigner presigner = S3Presigner.builder()
                .region(bucketRegion)
                .build()) {

            // Build the GetObjectRequest
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            // Build the GetObjectPresignRequest
            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(600)) // Set expiration time
                    .getObjectRequest(getObjectRequest)
                    .build();

            // Generate the pre-signed URL
            return presigner.presignGetObject(presignRequest).url().toString();
        } catch (Exception e) {
            System.err.println("[DEBUG] Error generating pre-signed URL: " + e.getMessage());
            return null;
        }
    }

    public int getNumOfHealthyWorkersInstances() {
        try {
            // Describe the EC2 instance by instanceId with a filter for its state (running)
            DescribeInstancesRequest describeRequest = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder()
                                    .name("tag:Name")
                                    .values("Worker")
                                    .build(),
                            Filter.builder()
                                    .name("instance-state-name")
                                    .values("running")
                                    .build()
                    ).build();

            DescribeInstancesResponse describeResponse = ec2.describeInstances(describeRequest);

            List<String> instanceIds = describeResponse.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .map(instance -> instance.instanceId())
                    .collect(Collectors.toList());

            return instanceIds.size();
        } catch (Exception e) {
            System.err.println("[DEBUG] Error checking num of healthy instances " + e.getMessage());
            return 0;
        }
    }

    public void terminateInstance(String instanceId) {
        try {
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            TerminateInstancesResponse terminateResponse = ec2.terminateInstances(terminateRequest);

            if (terminateResponse.terminatingInstances().size() > 0) {
                System.out.println("[DEBUG] Successfully terminated instance: " + instanceId);
            } else {
                System.out.println("[DEBUG] Failed to terminate instance: " + instanceId);
            }
        } catch (Ec2Exception e) {
            System.err.println("[DEBUG] Error stopping the instance: " + e.getMessage());
        }
    }

    public void deleteMessageFromSQS(String queueName, Message message) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String sqsURL = sqs.getQueueUrl(getQueueRequest).queueUrl();
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(sqsURL)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
        if (!queueName.equals(managerToWorkers) && !queueName.equals(workersToManager)) {
            System.out.println("[DEBUG] Message " + message.body() + " deleted from queue " + queueName);
        }
    }

    public void terminateWorkerInstances() {
        try {
            // Step 1: Describe instances with filters for tag "Worker" and state "running"
            DescribeInstancesRequest describeRequest = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder()
                                    .name("tag:Name")
                                    .values("Worker")
                                    .build(),
                            Filter.builder()
                                    .name("instance-state-name")
                                    .values("running")
                                    .build()
                    ).build();

            DescribeInstancesResponse describeResponse = ec2.describeInstances(describeRequest);

            List<String> instanceIds = describeResponse.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .map(instance -> instance.instanceId())
                    .collect(Collectors.toList());

            if (instanceIds.isEmpty()) {
                System.out.println("[INFO] No running Worker instances found to terminate.");
                return;
            }

            // Step 4: Terminate the stopped instances
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceIds)
                    .build();
            TerminateInstancesResponse terminateResponse = ec2.terminateInstances(terminateRequest);
            terminateResponse.terminatingInstances().forEach(instance -> {
                System.out.printf("[DEBUG] Terminating Worker EC2 Instance %s\n", instance.instanceId());
            });

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] Failed to gracefully terminate Worker instances: " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            System.err.println("[ERROR] Unexpected error: " + e.getMessage());
        }
    }

    public void terminateManager() {
        try {
            // Step 1: Describe EC2 instances with tag "Name=Manager"
            DescribeInstancesRequest describeRequest = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder().name("tag:Name").values("Manager").build(),
                            Filter.builder().name("instance-state-name").values("running").build()
                    )
                    .build();
            DescribeInstancesResponse describeResponse = ec2.describeInstances(describeRequest);

            List<String> managerInstanceIds = describeResponse.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .map(instance -> instance.instanceId())
                    .collect(Collectors.toList());

            if (managerInstanceIds.isEmpty()) {
                System.out.println("[INFO] No Manager instance found to terminate.");
                return;
            }

            String instanceId = managerInstanceIds.get(0); // Assuming one Manager instance

            System.out.printf("[DEBUG] Found Manager instance to terminate: %s\n", instanceId);

            // Step 3: Terminate the instance
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            TerminateInstancesResponse terminateResponse = ec2.terminateInstances(terminateRequest);

            terminateResponse.terminatingInstances().forEach(instance -> {
                System.out.printf("[DEBUG] Terminating Manager EC2 Instance: %s\n", instance.instanceId());
            });

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] Failed to terminate the Manager instance: " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            System.err.println("[ERROR] Unexpected error: " + e.getMessage());
        }
    }

// Deletes 'appToManager' and 'managerToApp' queues
    public void deleteApp_ManagerQueues() {
        deleteQueues("appToManager", "managerToApp");
    }

// Deletes 'managerToWorkers' and 'workersToManager' queues
    public void deleteManager_WorkersQueues() {
        deleteQueues("managerToWorkers", "workersToManager");
    }

// Helper method to delete the specified queues
    private void deleteQueues(String... queueNames) {
        try {
            System.out.println("[DEBUG] Deleting specified queues...");

            // Iterate through each queue name
            for (String queueName : queueNames) {
                GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build();

                String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

                DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build();

                sqs.deleteQueue(deleteQueueRequest);
                System.out.println("[DEBUG] Deleted SQS Queue: " + queueName);
            }

            System.out.println("[DEBUG] Specified queues deleted successfully.");

        } catch (SqsException e) {
            System.err.println("[ERROR] Error while deleting queues: " + e.awsErrorDetails().errorMessage());
        }
    }

    /*--------------------------------------------------Functions that were used for tests---------------------------------------------------------- */
    public void deleteContentOfBucket(String bucketName, String folderName) {
        try {
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder().bucket(bucketName).prefix(folderName).build();
            ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

            List<S3Object> objects = listObjectsResponse.contents();
            for (S3Object object : objects) {
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(object.key())
                        .build();
                s3.deleteObject(deleteObjectRequest);
                System.out.println("[DEBUG] Deleted object: " + object.key() + " from bucket: " + bucketName);
            }

            // Delete the bucket
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3.deleteBucket(deleteBucketRequest);
            System.out.println("[DEBUG] Deleted bucket: " + bucketName);

        } catch (Exception e) {
            System.err.println("[DEBUG] Failed to delete S3 buckets: " + e.getMessage());
        }
    }

// Delete all S3 buckets and their contents
    public void deleteAllS3Buckets() {
        try {
            ListBucketsResponse bucketsResponse = s3.listBuckets();

            List<Bucket> buckets = bucketsResponse.buckets();
            for (Bucket bucket : buckets) {
                String bucketName = bucket.name();

                // Delete all objects in the bucket
                ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder().bucket(bucketName).build();
                ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

                List<S3Object> objects = listObjectsResponse.contents();
                for (S3Object object : objects) {
                    DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(object.key())
                            .build();
                    s3.deleteObject(deleteObjectRequest);
                    System.out.println("[DEBUG] Deleted object: " + object.key() + " from bucket: " + bucketName);
                }

                // Delete the bucket
                DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
                s3.deleteBucket(deleteBucketRequest);
                System.out.println("[DEBUG] Deleted bucket: " + bucketName);
            }
        } catch (Exception e) {
            System.err.println("[DEBUG] Failed to delete S3 buckets: " + e.getMessage());
        }
    }

    public void gracefullyTerminateAllEc2Instances() {
        try {
            DescribeInstancesRequest describeRequest = DescribeInstancesRequest.builder().build();
            DescribeInstancesResponse describeResponse = ec2.describeInstances(describeRequest);

            List<Reservation> reservations = describeResponse.reservations();
            for (Reservation reservation : reservations) {
                List<String> instanceIds = reservation.instances().stream()
                        .map(instance -> instance.instanceId())
                        .collect(Collectors.toList());

                if (!instanceIds.isEmpty()) {
                    // Step 1: Stop the instances
                    StopInstancesRequest stopRequest = StopInstancesRequest.builder()
                            .instanceIds(instanceIds)
                            .build();
                    ec2.stopInstances(stopRequest);
                    System.out.println("Stopping EC2 Instances: " + instanceIds);

                    // Step 2: Wait until all instances are stopped
                    for (String instanceId : instanceIds) {
                        ec2.waiter().waitUntilInstanceStopped(
                                DescribeInstancesRequest.builder().instanceIds(instanceId).build()
                        );
                        System.out.println("Instance stopped: " + instanceId);
                    }

                    // Step 3: Terminate the instances
                    TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                            .instanceIds(instanceIds)
                            .build();
                    TerminateInstancesResponse response = ec2.terminateInstances(terminateRequest);
                    response.terminatingInstances().forEach(instance -> {
                        System.out.println("Terminating EC2 Instance: " + instance.instanceId());
                    });
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to gracefully terminate EC2 instances: " + e.getMessage());
        }
    }

    public void deleteAllMessagesFromAllQueues() {
        try {
            System.out.println("[DEBUG] Listing all queues...");

            // List all queues
            ListQueuesResponse listQueuesResponse = sqs.listQueues();
            List<String> queueUrls = listQueuesResponse.queueUrls();

            if (queueUrls.isEmpty()) {
                System.out.println("[DEBUG] No SQS queues found.");
                return;
            }

            // Iterate through each queue
            for (String queueUrl : queueUrls) {
                System.out.println("[DEBUG] Clearing messages from queue: " + queueUrl);

                // Receive and delete messages in batches
                while (true) {
                    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .maxNumberOfMessages(10) // Retrieve up to 10 messages (SQS limit)
                            .build();

                    List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();

                    if (messages.isEmpty()) {
                        System.out.println("[DEBUG] No more messages in queue: " + queueUrl);
                        break;
                    }

                    for (Message message : messages) {
                        deleteMessageFromSQS(queueUrl, message);
                    }
                }
            }

            System.out.println("[DEBUG] All messages cleared from all queues.");

        } catch (SqsException e) {
            System.err.println("[ERROR] Error while clearing messages: " + e.awsErrorDetails().errorMessage());
        }
    }

    public void stopInstance(String instanceId) {
        try {
            StopInstancesRequest stopRequest = StopInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            StopInstancesResponse stopResponse = ec2.stopInstances(stopRequest);

            if (stopResponse.stoppingInstances().size() > 0) {
                System.out.println("[DEBUG] Successfully stopped instance: " + instanceId);
            } else {
                System.out.println("[DEBUG] Failed to stop instance: " + instanceId);
            }
        } catch (Ec2Exception e) {
            System.err.println("[DEBUG] Error stopping the instance: " + e.getMessage());
        }
    }

    public int startNStoppedInstances(int n) {
        int startedInstancesCount = 0;

        try {
            // Step 1: Describe all stopped instances
            DescribeInstancesRequest describeRequest = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder()
                                    .name("tag:Name")
                                    .values("Worker")
                                    .build(),
                            Filter.builder()
                                    .name("instance-state-name")
                                    .values("stopped")
                                    .build()
                    ).build();

            DescribeInstancesResponse describeResponse = ec2.describeInstances(describeRequest);

            // Step 2: Collect instance IDs of stopped instances
            List<String> stoppedInstanceIds = describeResponse.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .map(Instance::instanceId)
                    .filter(instanceId -> instanceId != null && !instanceId.isEmpty()) // Filter out null or empty IDs
                    .collect(Collectors.toList());

            if (stoppedInstanceIds.isEmpty()) {
                System.out.println("[DEBUG] No stopped instances found.");
                return 0;
            }

            // Step 3: Take up to 'n' instances to start
            List<String> instancesToStart = stoppedInstanceIds.stream()
                    .limit(n)
                    .collect(Collectors.toList());

            // Step 4: Start the instances
            StartInstancesRequest startRequest = StartInstancesRequest.builder()
                    .instanceIds(instancesToStart)
                    .build();

            StartInstancesResponse startResponse = ec2.startInstances(startRequest);

            // Step 5: Count the successfully started instances
            startedInstancesCount = startResponse.startingInstances().size();

            // Log details of the started instances
            startResponse.startingInstances().forEach(instanceStateChange
                    -> System.out.println("[DEBUG] Started instance: " + instanceStateChange.instanceId())
            );

        } catch (Ec2Exception e) {
            System.err.println("[DEBUG] Error while starting instances: " + e.getMessage());
            return 0;
        }

        return startedInstancesCount;
    }

}
