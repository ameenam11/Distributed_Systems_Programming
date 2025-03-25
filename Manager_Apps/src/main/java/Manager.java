import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import software.amazon.awssdk.services.sqs.model.Message;

public class Manager {
    final static AWS aws = AWS.getInstance();

    private final static int maxNumOfWorkers = 8;
    private final static int numOfThreads = 4;
    /* 
       We've checked the number of CPU cores of an ec2 instance and it is 2. So, after reading a little bit about the optimal amount of threads to use 
       in our program we have concluded that 4 threads at each pool is the optimal one. Of course, in a larger CPU we could've added more threads and 
       the program would be more effecient.
    */      
    
    private static Object lockForActiveWorkers = new Object();
    private static Object lockForStoppedWorkers = new Object();

    private static volatile  int numOfActiveWorkers = 0;
    private static volatile double tasksPerWorker;
    private static volatile boolean terminateMode = false;
    private static volatile boolean isAppsThreadTerminated = false;

    private static ExecutorService appsThreadPool;
    private static ExecutorService workersThreadPool;

    private static ConcurrentHashMap<String, AppsProcessingState> appsStates = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Boolean> workerHealthMap = new ConcurrentHashMap<>();
    private static Vector<String> apps = new Vector<>();


    public static void main(String[] args) {
//        System.out.println("----------------Setting Up The Manager----------------");
//        setup(args);
//        System.out.println("----------------Finished Setting Up the manager----------------");
        aws.createSqsQueue(aws.managerToWorkers);
        aws.createSqsQueue(aws.workersToManager);

        new Thread(new Runnable() {             // Apps Listener
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    List<Message> messages = aws.getMessagesFromSqs(aws.appToManager, 20);

                    for (Message message : messages) {
                        String[] content = parseMessage(message.body());
                        String appId = content[0];
                        if(content.length > 2){
                            if(!apps.contains(appId)){
                                apps.add(appId);

                                appsThreadPool.submit(() -> {
                                    processMessageFromApp(message);
                                });
                            }
                        }else{
                            terminateMode = true;
                            aws.deleteMessageFromSQS(aws.appToManager, message);
                            break;
                        }
                        
                    }
                    if (terminateMode)
                        break;
                }
            
                isAppsThreadTerminated = true;
                System.out.println("[DEBUG] Apps listener thread has been terminated.");
                System.out.println();
            }
        }).start();

        new Thread(new Runnable() {             // Workers Listener
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    List<Message> messages = aws.getMessagesFromSqs(aws.workersToManager, 20);
                    for (Message message : messages) {
                        workersThreadPool.submit(() -> {
                            processMessageFromWorker(message);
                        });
                    }
                    if(terminateMode && isAppsThreadTerminated && 
                            allWorkersFinishedTheirWork() && allActiveAppsTookSummaryFiles())
                        break;
                }
                
                aws.terminateWorkerInstances();
                aws.deleteManager_WorkersQueues();

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }

                appsThreadPool.shutdown();
                workersThreadPool.shutdown();

                aws.deleteApp_ManagerQueues();

                System.out.println("[DEBUG] Terminating the Manager instance..");
                aws.terminateManager();
            }
        }).start();



        new Thread(() -> {                      // Workers health checker  
            while (!terminateMode) {
                try {
                    Thread.sleep(120000); // Check every 30 seconds
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                checkWorkersHealth();
            }
        }).start();
        

        System.out.println("[DEBUG] Activating the pools..");
        appsThreadPool = Executors.newFixedThreadPool(numOfThreads);
        workersThreadPool = Executors.newFixedThreadPool(numOfThreads);
    }

    private static String[] parseMessage(String message){
        return message.split("\t");
    }

    private static boolean allWorkersFinishedTheirWork(){
        return apps.size() == 0;
    }

    private static boolean allActiveAppsTookSummaryFiles(){
        return aws.getApproximateNumberOfMessages(aws.managerToApp) == 0;
    }


    private static void processMessageFromApp(Message message) { // message.body = "id URL tasksPerWorker"
        String[] content = parseMessage(message.body());
        String appId = content[0];
        String s3URL = content[1];
        tasksPerWorker = Integer.parseInt(content[2]);

        try {
            System.out.println("[DEBUG] Processing message: " + message.body());

            String filePath = "ManagerFiles/" + appId + ".txt";
            aws.downloadFileFromS3(aws.bucketForFiles, s3URL, filePath);            //----------CHECK PLEASE
            int lines_counter = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    aws.sendMessage(aws.managerToWorkers, appId + "\t" + line);
                    ++lines_counter;
                }
                

                int temp = ((int) (Math.ceil(lines_counter / tasksPerWorker)));
                int requiredNumOfWorkers = temp > maxNumOfWorkers ? maxNumOfWorkers : temp;
                
                System.out.println("[DEBUG] The amount of workers needed for this job: " + temp);
                

                synchronized (lockForActiveWorkers) {
                    if (numOfActiveWorkers != 0) {
                        if (requiredNumOfWorkers >= numOfActiveWorkers)
                            requiredNumOfWorkers -= numOfActiveWorkers;
                    }
                    
                    // requiredNumOfWorkers -= aws.startNStoppedInstances(requiredNumOfWorkers);
                    synchronized (lockForStoppedWorkers) {
                        for(int i = 0; i < requiredNumOfWorkers && numOfActiveWorkers < maxNumOfWorkers; i++){
                            String instanceID = aws.createEC2(aws.getData("Worker"), "Worker", 1); 
                            workerHealthMap.put(instanceID, true);
                            ++numOfActiveWorkers;
                        
                        }
                    }
                }
                System.out.println("[DEBUG] Successfully sent all lines.");
                System.out.println();

                appsStates.put(appId, new AppsProcessingState(lines_counter));

            } catch (IOException e) {
                System.err.println("[DEBUG] Error reading file: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("[DEBUG] Error processing message: " + e.getMessage());
        }
        
        aws.deleteMessageFromSQS(aws.appToManager, message);

    }


    private static void processMessageFromWorker(Message message) { // message.body = "id op: inputFileLink outputFileLink"
        String[] content = message.body().split("\t");
        String appId = content[0];
        String processedLine = content[1] + "\t" +  content[2] + "\t" + content[3]; // op + inputFileLink + outputFileLink

        AppsProcessingState state = appsStates.get(appId);
        if (state != null) {
            state.addProcessedLine(processedLine);

            state.decrementLineCount();

            if (state.isComplete()) {
                System.out.println("[DEBUG] All lines processed for appID: " + appId);

                combineLinesAndUpload(state.getProcessedLines(), appId);

                appsStates.remove(appId);
                apps.remove(appId);
            }
        }

        aws.deleteMessageFromSQS(aws.workersToManager, message);
    }

    private static void combineLinesAndUpload(ConcurrentLinkedQueue<String> processedLines, String appID) {
        String summaryFilePath = "ManagerFiles/summary_" + appID + ".txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFilePath))) {
            for (String line : processedLines) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String s3Url = aws.uploadFileToS3(aws.bucketForFiles, summaryFilePath, "app" + appID + "/");

        aws.sendMessage(aws.managerToApp, appID + "\t" + s3Url);
    }

    private static void checkWorkersHealth() {
        System.out.println("entered checkHealth");
            try {
            System.out.println("checking workers health");
            int numOfHealthyWorkers = aws.getNumOfHealthyWorkersInstances();
            synchronized (lockForStoppedWorkers) {
                while(numOfHealthyWorkers < numOfActiveWorkers){
                    aws.createEC2(aws.getData("Worker"), "Worker", 1);
                    ++numOfHealthyWorkers;
                }
            }
        } catch (Exception e) {
            System.err.println("[DEBUG] Error checking worker " + e.getMessage());
        }
    }
    
    


}