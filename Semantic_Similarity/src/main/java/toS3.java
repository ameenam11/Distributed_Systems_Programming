import java.io.File;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class toS3 {

    public static void main(String[] args) {
        String bucketName = "ameen-raya-hw3/jars";
        String filePath = args[0];
        String keyName = args[1];

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();

            // Upload the file
            PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(filePath));
            s3Client.putObject(request);
            System.out.println("File uploaded successfully to " + bucketName + "/" + keyName);
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process it
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response
            e.printStackTrace();
        }
    }
}