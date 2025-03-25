import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

public class LocalApp {
    final static AWS aws = AWS.getInstance();

    private static String inputFileName;
    private static String outputFileName;
    private static String id;
    private static int tasksPerWorker;
    private static boolean terminateMode = false;


    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
        try {
            System.out.println("----------------Setting Up The Local Application----------------");
            
            id = "" + System.currentTimeMillis();
            System.out.println("[DEBUG] Your ID is: " + id);
            
            boolean isValid = setup(args);

            if (isValid) {
                System.out.println("----------------Finished Setting Up----------------");
                System.out.println("----------------Waiting for summary file----------------");
                boolean isSummaryFileFound = false;
                try{
                while (!isSummaryFileFound) {
                    List<Message> messages = aws.getMessagesFromSqs(aws.managerToApp, 10);

                    for (Message message : messages) {
                        String[] content = message.body().split("\t");
                        if (id.equals(content[0])) {
                            System.out.println("[DEBUG] Summary file is detected for App: " + id);
                            String localFilePath = "localAppOutputFiles/" + outputFileName + "(" + id + ").txt";
                            aws.downloadFileFromS3(aws.bucketForFiles, content[1], localFilePath);
                            String outputHtmlPath = "HTML-outputFiles/" + outputFileName + "(" + id + ").html";
                            createHtmlFromFile(localFilePath, outputHtmlPath);
                            isSummaryFileFound = true;
                            aws.deleteMessageFromSQS(aws.managerToApp, message);
                            if(terminateMode){
                                aws.sendMessage(aws.appToManager, id + "\tterminate");
                            }
                            break;
                        }
                    }
                }
                }catch (QueueDoesNotExistException e){
                    if (e.getMessage().contains("The specified queue does not exist for this wsdl version")) {
                        System.out.println("[DEBUG] The Manager was terminated due to a termination message that was sent. Sorry.");
                    }
                }
                
                end();
            }
        } catch(Exception e){
            System.out.println("An error occured during the setup of the program. Please try again.");
        }

    }

    //Create Buckets, Create Queues, Upload JARs to S3
    public static boolean setup(String[] args) {
        if(args.length < 3 || args.length > 4){
            System.out.println("[DEBUG] Invalid number of arguments.");
            return false;
        }
        if(args.length == 4) terminateMode = true;

        inputFileName = args[0];
        outputFileName = args[1];
        tasksPerWorker = Integer.parseInt(args[2]);

        
        aws.createBucketIfNotExists(aws.bucketForFiles);
        aws.createBucketIfNotExists(aws.bucketForJars);

        aws.uploadFileToS3(aws.bucketForJars, "target/Manager.jar", "");
        aws.uploadFileToS3(aws.bucketForJars, "target/Worker.jar", "");

        aws.createSqsQueue(aws.appToManager);
        aws.createSqsQueue(aws.managerToApp);

        createManagerEC2();
    
        String inputFilePath = "localAppInputFiles/" + inputFileName + ".txt";
        String message = id + "\t" + aws.uploadFileToS3(aws.bucketForFiles, inputFilePath, "app" + id + "/") + "\t" + tasksPerWorker;

        aws.sendMessage(aws.appToManager, message);
        return true;
    }

    private static void createManagerEC2() {
        System.out.println("[DEBUG] activating the EC2 Manager.");
        boolean isActive = aws.checkIfManagerIsActive();

        if(!isActive) { //      ----------------------------MODIFY THE EC2SCRIPT----------------------------
            String ec2Script = aws.getData("Manager");
            String managerInstanceID = aws.createEC2(ec2Script, "Manager", 1);
        }else
            System.out.println("[DEBUG] The Manager instance is already activated.");
        System.out.println();
    }

    public static void createHtmlFromFile(String inputFilePath, String outputHtmlPath) {
        try {
            // Read the lines of the file
            Path path = Paths.get(inputFilePath);
            List<String> lines = Files.readAllLines(path);
    
            // Start building the HTML content
            StringBuilder htmlContent = new StringBuilder();
            htmlContent.append("<html>\n")
                    .append("<head>\n")
                    .append("<title>Summary File</title>\n")
                    .append("<style>\n")
                    .append("    body { font-family: Arial, sans-serif; margin: 20px; }\n")
                    .append("    h1 { color: #333333; }\n")
                    .append("    p { color: #555555; }\n")
                    .append("    .error { color: red; }\n")
                    .append("    .success { color: green; }\n")
                    .append("</style>\n")
                    .append("</head>\n")
                    .append("<body>\n")
                    .append("<h1>Summary of the File</h1>\n");
    
            // Process each line and append formatted content to HTML
            for (String line : lines) {
                String[] parts = line.split("\t");
    
                if (parts.length == 3 && (parts[2].toLowerCase().contains("error") || parts[2].toLowerCase().contains("failed"))) {
                    String operation = parts[0];
                    String inputFileLink = parts[1];
                    String errorMessage = parts[2];
                    htmlContent.append("<p class='error'>")
                            .append(operation).append("\t").append(inputFileLink).append("\t").append(errorMessage)
                            .append("</p>\n");
                } else if (parts.length == 3) {
                    String operation = parts[0];
                    String inputFileLink = parts[1];
                    String outputFile = parts[2];
                    htmlContent.append("<p class='success'>")
                            .append(operation).append("\t").append(inputFileLink).append("\t").append(outputFile)
                            .append("</p>\n");
                } else {
                    // Handle unexpected format gracefully
                    htmlContent.append("<p>").append(line).append("</p>\n");
                }
            }
    
            // End the HTML content
            htmlContent.append("</body>\n</html>");
    
            // Write the HTML content to a file
            Files.write(Paths.get(outputHtmlPath), htmlContent.toString().getBytes());
            System.out.println("HTML file created: " + outputHtmlPath);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error reading file or creating HTML.");
        }
    }
    
    
    public static void end(){
        System.out.println("[DEBUG] Bye Bye..");
    }

}