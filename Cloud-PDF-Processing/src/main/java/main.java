public class main {
    final static AWS aws = AWS.getInstance();
        public static void main(String[] args){
            // aws.deleteAllMessagesFromAllQueues();
            // //aws.deleteAllSqsQueues();
            // aws.deleteAllS3Buckets();

            // aws.gracefullyTerminateAllEc2Instances();
            System.out.println(Runtime.getRuntime().availableProcessors());
            
        }
}
