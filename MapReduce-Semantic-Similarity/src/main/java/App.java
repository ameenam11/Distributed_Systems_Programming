import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class App {
        public static AWSCredentialsProvider credentialsProvider;
        public static AmazonS3 S3;
        public static AmazonEC2 ec2;
        public static AmazonElasticMapReduce emr;

        public static int numberOfInstances = 9;

        public static void main(String[] args) {
                credentialsProvider = new ProfileCredentialsProvider();
                System.out.println("[INFO] Connecting to aws");
                ec2 = AmazonEC2ClientBuilder.standard()
                                .withCredentials(credentialsProvider)
                                .withRegion("us-east-1")
                                .build();
                S3 = AmazonS3ClientBuilder.standard()
                                .withCredentials(credentialsProvider)
                                .withRegion("us-east-1")
                                .build();
                emr = AmazonElasticMapReduceClientBuilder.standard()
                                .withCredentials(credentialsProvider)
                                .withRegion("us-east-1")
                                .build();
                System.out.println("list cluster");
                System.out.println(emr.listClusters());

                // Step 1
                HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                                .withJar("s3://ameen-raya-hw3/jars/Step1.jar")
                                .withMainClass("Step1")
                                .withArgs("s3://biarcs/");

                StepConfig stepConfig1 = new StepConfig()
                                .withName("Step1")
                                .withHadoopJarStep(step1)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                // Step 2
                HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                                .withJar("s3://ameen-raya-hw3/jars/Step2.jar")
                                .withMainClass("Step2");

                StepConfig stepConfig2 = new StepConfig()
                                .withName("Step2")
                                .withHadoopJarStep(step2)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                // // Step 3
                HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                                .withJar("s3://ameen-raya-hw3/jars/Step3.jar")
                                .withMainClass("Step3");

                StepConfig stepConfig3 = new StepConfig()
                                .withName("Step3")
                                .withHadoopJarStep(step3)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                // Job flow
                JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                                .withInstanceCount(numberOfInstances)
                                .withMasterInstanceType(InstanceType.M4Large.toString())
                                .withSlaveInstanceType(InstanceType.M4Large.toString())
                                .withHadoopVersion("2.9.2")
                                .withEc2KeyName("vockey")
                                .withKeepJobFlowAliveWhenNoSteps(false)
                                .withPlacement(new PlacementType("us-east-1a"));

                System.out.println("Set steps");
                RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                                .withName("Map reduce hw3")
                                .withInstances(instances)
                                .withSteps(stepConfig1)
                                .withSteps(stepConfig2)
                                .withSteps(stepConfig3)
                                .withLogUri("s3://ameen-raya-hw3/logs10Percent/")
                                .withServiceRole("EMR_DefaultRole")
                                .withJobFlowRole("EMR_EC2_DefaultRole")
                                .withReleaseLabel("emr-5.11.0");

                RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
                String jobFlowId = runJobFlowResult.getJobFlowId();
                System.out.println("Ran job flow with id: " + jobFlowId);
        }
}
