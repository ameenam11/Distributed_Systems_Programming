import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDeletedRecentlyException;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

public class Worker {
    final static AWS aws = AWS.getInstance();

    private static String appID;

    public static void main(String[] args) {
        System.out.println("[DEBUG] Setting Up Worker");
        while (true) {
            try {
                while(aws.getApproximateNumberOfMessages(aws.managerToWorkers) > 0){
                    List<Message> messages = aws.getMessagesFromSqs(aws.managerToWorkers, 15);

                    for (Message message : messages) {
                        processMessage(message);
                    }
                }
                // aws.stopInstance(aws.getCurrentInstanceId());

            } catch (QueueDoesNotExistException | QueueDeletedRecentlyException e ) {
                System.err.println("[ERROR] Unexpected error: " + e.getMessage());
                break;
            }

            try {
                Thread.sleep(1000);     // let the worker sleep for 3 seconds if no messages were detected from the manager
            } catch (InterruptedException e) {
                break;
            }
        }
        System.out.println("[DEBUG] Worker terminated successfully!");
    }

    public static void processMessage(Message message){
        String[] parts = message.body().split("\t");
        appID = parts[0];
        String operation = parts[1];
        String pdfUrl = parts[2];

        try {
            System.out.println("[DEBUG] Processing: " + operation + " on " + pdfUrl);
            File pdfFile = downloadPDF(pdfUrl, "WorkerFiles", "file_" + appID + ".pdf");
            File resultFile = performOperation(pdfFile, operation);
            aws.uploadFileToS3(aws.bucketForFiles, resultFile.getAbsolutePath(), "app" + appID + "/processed-files/");
            send2Manager(operation, pdfUrl, aws.generatePresignedUrl(aws.bucketForFiles, "app" + appID + "/" + resultFile.getName()));
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to process message: " + e.getMessage());
            send2ManagerError(operation, pdfUrl, e.getMessage());
        }
        aws.deleteMessageFromSQS(aws.managerToWorkers, message);
    }

    public static File downloadPDF(String pdfUrl, String localDir, String fileName) throws IOException {
        // Create the target file
        Path filePath = Paths.get(localDir, fileName);
        File file = filePath.toFile();
        System.out.println("[DEBUG] Attempting to download PDF from: " + pdfUrl);
        System.out.println("[DEBUG] Local file path: " + file.getAbsolutePath());

        try {
            // Validate URL
            URL url = new URL(pdfUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set User-Agent to mimic a browser
            connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

            // Configure connection
            connection.setRequestMethod("GET");
            connection.setInstanceFollowRedirects(true);
            connection.setConnectTimeout(5000); // 5 seconds timeout
            connection.setReadTimeout(5000);    // 5 seconds timeout

            // Check HTTP response code
            int responseCode = connection.getResponseCode();
            System.out.println("[DEBUG] HTTP Response Code: " + responseCode);
            if (responseCode != 200) {
                throw new IOException("Failed to connect, HTTP response code: " + responseCode);
            }

            // Validate content type
            String contentType = connection.getContentType();
            System.out.println("[DEBUG] Content Type: " + contentType);
            if (!"application/pdf".equals(contentType)) {
                throw new IOException("Error: Unexpected content type: " + contentType);
            }

            // Download file
            try (InputStream inputStream = connection.getInputStream()) {
                Files.copy(inputStream, filePath, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("[DEBUG] PDF downloaded successfully to: " + file.getAbsolutePath());
            }
        } catch (Exception e) {
            throw new IOException("Failed to download PDF from URL: " + pdfUrl, e);
        }

        return file;
    }





    private static File performOperation (File pdfFile, String operation) throws IOException {
        switch (operation) {
            case "ToImage":
                return convertToImage(pdfFile);
            case "ToHTML":
                return convertToHTML(pdfFile);
            case "ToText":
                return convertToText(pdfFile);
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    private static File convertToImage(File pdfFile) throws IOException {
        File imageFile = File.createTempFile("converted", ".png");
        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFRenderer renderer = new PDFRenderer(document);

            // Render the first page (index 0) at 150 DPI
            RenderedImage image = renderer.renderImageWithDPI(0, 150);

            // Save the image to the temporary PNG file
            ImageIO.write(image, "PNG", imageFile);
        } catch (IOException e) {
            System.err.println("Error converting PDF to image: " + e.getMessage());
            throw e;
        }
        return imageFile;
    }


    private static File convertToHTML(File pdfFile) throws IOException {
        File htmlFile = File.createTempFile("converted", ".html");
        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFTextStripper textStripper = new PDFTextStripper();
            textStripper.setStartPage(1); // Extract only the first page
            textStripper.setEndPage(1);

            String text = textStripper.getText(document);
            String htmlContent = "<html><body><pre>" + text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre></body></html>";

            // Write the HTML content to the temporary file
            Files.write(htmlFile.toPath(), htmlContent.getBytes(), StandardOpenOption.CREATE);
        } catch (IOException e) {
            System.err.println("Error converting PDF to HTML: " + e.getMessage());
            throw e;
        }
        return htmlFile;
    }


    private static File convertToText(File pdfFile) throws IOException {
        File textFile = File.createTempFile("converted", ".txt");
        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFTextStripper textStripper = new PDFTextStripper();
            textStripper.setStartPage(1); // Extract only the first page
            textStripper.setEndPage(1);

            String text = textStripper.getText(document);

            // Write the text to the temporary file
            Files.write(textFile.toPath(), text.getBytes(), StandardOpenOption.CREATE);
        } catch (IOException e) {
            System.err.println("Error converting PDF to text: " + e.getMessage());
            throw e;
        }
        return textFile;
    }


    private static void send2Manager(String operation, String originalUrl, String resultUrl){
        String message = appID + "\t" + String.format("%s:\t%s\t%s", operation, originalUrl, resultUrl);
        aws.sendMessage(aws.workersToManager, message);
    }

    private static void send2ManagerError(String operation, String originalUrl, String errorMessage){
        String message = appID + "\t" + String.format("%s:\t%s\t%s", operation, originalUrl, errorMessage);
        aws.sendMessage(aws.workersToManager, message);
    }
}
