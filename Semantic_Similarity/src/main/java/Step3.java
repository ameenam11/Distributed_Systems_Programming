import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> lexemes = new HashSet<>();
        private Map<String, String> wordRelatedNess  = new HashMap<>();
        private Stemmer stemmer = new Stemmer();

        protected void setup(Context context) throws IOException, InterruptedException {
                super.setup(context);
                FileSystem fs = null;
                BufferedReader br = null;
                try{
                    Configuration conf = context.getConfiguration();
                    fs = FileSystem.get(new URI("s3a://ameen-raya-hw3/"), conf);
                    Path path = new Path("s3a://ameen-raya-hw3/lexemes_count.txt");
                    // Path path = new Path("s3a://ameen-raya-hw3/out_step2/part-r-00000");
                    br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        String lexeme = parts[0];
                        // String lexeme = line.split("\t")[0].split(",")[1];
                        lexemes.add(lexeme);
                    }

                    path = new Path("s3a://ameen-raya-hw3/word-relatedness.txt");
                    br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        String l1 = parts[0];
                        String l2 = parts[1];
                        //stem l1 & l2
                        stemmer.add(l1.toCharArray(), l1.length());
                        stemmer.stem();
                        l1 = stemmer.toString();
                        stemmer.add(l2.toCharArray(), l2.length());
                        stemmer.stem();
                        l2 = stemmer.toString();
                        String pair = l1 + " " + l2;
                        String relatedness = parts[2];
                        wordRelatedNess.put(pair, relatedness);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) {
                        br.close();
                    }
                    if (fs != null) {
                        fs.close();
                    }
                }
        }
        // @Override
        // public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //     String line = key.toString();
        //     String[] pair = line.split(",");
        //     int eqNum = Integer.parseInt(pair[0]);
        //     String lexeme1 = pair[1];

        //     for(String lexeme2 : lexemes){
        //         if(lexeme1.equals(lexeme2)){
        //             continue;
        //         }
                
        //         if(lexeme1.compareTo(lexeme2) < 0){
        //             context.write(new Text(lexeme1 + " " + lexeme2 + " " + eqNum), value);
        //         }else{
        //             context.write(new Text(lexeme2 + " " + lexeme1 + " " + eqNum), value);
        //         }
        //     }
        // }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            String[] pair = parts[0].split(",");
            int eqNum = Integer.parseInt(pair[0]);
            String lexeme1 = pair[1];

            for(String lexeme2 : lexemes){
                if(lexeme1.equals(lexeme2)){
                    continue;
                }
                
                if(lexeme1.compareTo(lexeme2) < 0){
                    if(wordRelatedNess.containsKey(lexeme1 + " " + lexeme2))
                        context.write(new Text(lexeme1 + " " + lexeme2 + " " + eqNum + " " + wordRelatedNess.get(lexeme1 + " " + lexeme2)), new Text(parts[1]));
                }else{
                    if(wordRelatedNess.containsKey(lexeme2 + " " + lexeme1))
                        context.write(new Text(lexeme2 + " " + lexeme1 + " " + eqNum + " " + wordRelatedNess.get(lexeme2 + " " + lexeme1)), new Text(parts[1]));
                }
            }
        }
    }
    


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private String lexeme1 = null;
        private String lexeme2 = null;
        private StringBuilder sb = new StringBuilder();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String[] keyParts = key.toString().split(" ");
                lexeme1 = keyParts[0];
                lexeme2 = keyParts[1];
                int eqNum = Integer.parseInt(keyParts[2]);
                String relatedness = keyParts[3];

                String[] v1 = values.iterator().next().toString().split(" ");
                String[] v2 = values.iterator().next().toString().split(" ");

                double cosineSimilarity = computeCosineSimilarity(v1, v2);
                double jaccardSimilarity = computeJaccard(v1, v2);
                double euclideanDistance = computeEuclideanDistance(v1, v2);
                double diceSimilarity = computeDiceSimilarity(v1, v2);
                double overlapSimilarity = computeOverlapSimilarity(v1, v2);
                double manhattanDistance = computeManhattanDistance(v1, v2);

                sb.append(cosineSimilarity).append(" ")
                        .append(jaccardSimilarity).append(" ")
                        .append(euclideanDistance).append(" ")
                        .append(diceSimilarity).append(" ")
                        .append(overlapSimilarity).append(" ")
                        .append(manhattanDistance);
                
                if(eqNum == 8){
                    sb.append("\t").append(relatedness);
                    context.write(new Text(lexeme1 + " " + lexeme2), new Text(sb.toString()));
                    sb = new StringBuilder();
                }else
                    sb.append(" ");
        
        }


        private double computeCosineSimilarity(String[] v1, String[] v2) {
            double dotProduct = 0.0, normV1 = 0.0, normV2 = 0.0;
            if (v1.length == 0 || v2.length == 0) return 0.0; // Edge case
        
            for (int i = 0; i < v1.length; i++) {
                double num1 = Double.parseDouble(v1[i]);
                double num2 = Double.parseDouble(v2[i]);
                dotProduct += num1 * num2;
                normV1 += num1 * num1;
                normV2 += num2 * num2;
            }
        
            double denominator = Math.sqrt(normV1) * Math.sqrt(normV2);
            return (denominator == 0.0) ? 0.0 : dotProduct / denominator; // Avoid division by zero
        }
        
        private double computeJaccard(String[] v1, String[] v2) {
            double intersection = 0.0, union = 0.0;
            if (v1.length == 0 || v2.length == 0) return 0.0; // Edge case
        
            for (int i = 0; i < v1.length; i++) {
                double num1 = Double.parseDouble(v1[i]);
                double num2 = Double.parseDouble(v2[i]);
                if (num1 > 0 && num2 > 0) intersection += 1;
                if (num1 > 0 || num2 > 0) union += 1;
            }
        
            return (union == 0.0) ? 0.0 : intersection / union; // Avoid division by zero
        }
        
        private double computeEuclideanDistance(String[] v1, String[] v2) {
            double sum = 0.0;
            if (v1.length == 0 || v2.length == 0) return 0.0; // Edge case
        
            for (int i = 0; i < v1.length; i++) {
                double num1 = Double.parseDouble(v1[i]);
                double num2 = Double.parseDouble(v2[i]);
                sum += Math.pow(num1 - num2, 2);
            }
        
            return Math.sqrt(sum);
        }
        
        private double computeDiceSimilarity(String[] v1, String[] v2) {
            double intersection = 0.0, sum = 0.0;
            if (v1.length == 0 || v2.length == 0) return 0.0; // Edge case
        
            for (int i = 0; i < v1.length; i++) {
                double num1 = Double.parseDouble(v1[i]);
                double num2 = Double.parseDouble(v2[i]);
                if (num1 > 0 && num2 > 0) intersection += 1;
                sum += num1 + num2;
            }
        
            return (sum == 0.0) ? 0.0 : (2 * intersection) / sum; // Avoid division by zero
        }
        
        private double computeOverlapSimilarity(String[] v1, String[] v2) {
            double minSum = 0.0, maxSum = 0.0;
            if (v1.length == 0 || v2.length == 0) return 0.0; // Edge case
        
            for (int i = 0; i < v1.length; i++) {
                double num1 = Double.parseDouble(v1[i]);
                double num2 = Double.parseDouble(v2[i]);
                minSum += Math.min(num1, num2);
                maxSum += Math.max(num1, num2);
            }
        
            return (maxSum == 0.0) ? 0.0 : minSum / maxSum; // Avoid division by zero
        }
        
        private double computeManhattanDistance(String[] v1, String[] v2) {
            double sum = 0.0;
            if (v1.length == 0 || v2.length == 0) return 0.0; // Edge case
        
            for (int i = 0; i < v1.length; i++) {
                double num1 = Double.parseDouble(v1[i]);
                double num2 = Double.parseDouble(v2[i]);
                sum += Math.abs(num1 - num2);
            }
        
            return sum;
        }
        
    }

    public static class WordPairPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split(" ");
            if (parts.length < 2) {
                return 0; // Default partition for malformed keys
            }
    
            String prefix = parts[0] + " " + parts[1]; // Extract "w1 w2"
    
            return (prefix.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    
    
    public static class WordPairComparator extends WritableComparator {
        protected WordPairComparator() {
            super(Text.class, true);
        }
    
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String[] parts1 = w1.toString().split(" ");
            String[] parts2 = w2.toString().split(" ");
    
            // Extract word pairs (first two words) and numerical values
            String wordPair1 = parts1[0] + " " + parts1[1];
            String wordPair2 = parts2[0] + " " + parts2[1];
            int cmp = wordPair1.compareTo(wordPair2);
            if (cmp != 0) return cmp;
    
            // Extract numerical values (assuming they are at the end of the string)
            double num1 = Double.parseDouble(parts1[2].replaceAll("[(),]", ""));
            double num2 = Double.parseDouble(parts2[2].replaceAll("[(),]", ""));
    
            return Double.compare(num1, num2); // Descending order
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort trigrams");
    
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
    
        job.setPartitionerClass(WordPairPartitioner.class);
        job.setSortComparatorClass(WordPairComparator.class);
    
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        // FileInputFormat.addInputPath(job, new Path("s3://ameen-raya-hw3/out_step2/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("s3://ameen-raya-hw3/out_step2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://ameen-raya-hw3/out_step3"));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
