import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Step2 {
        public static class MapperClass extends Mapper<Text, Text, Text, Text> {
            public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
                /*
                key = ngram ("C0", "1gram", "2gram", "3gram").
                value = the associated data:
                        - For "C0", value = count
                        - For 1grams, value = count:triplet
                        - For 2grams, value = count:triplet
                        - For 3grams, value = count
                */
            
                String[] tokens = value.toString().split(":");
                String count = tokens[0];
            
                if (key.toString().equals("C0")) {
                    String triplet = tokens[1].replace(",", " ");
                    String tripletContent = triplet.substring(1, triplet.length() - 1); 
                    context.write(new Text(tripletContent), new Text("C0=" + count));
                    return;
                }
            
                if (tokens.length == 1) {
                    context.write(key, new Text("N3=" + count));
                } else if (tokens.length == 2) {
                    String triplet = tokens[1];
                    String tripletContent = triplet.substring(1, triplet.length() - 1); 
                    String[] wordsOfTriplet = tripletContent.split(",");
                    String[] grams = key.toString().split(" ");
                    String result = "";
            
                    if (grams.length == 1) {
                        if (grams[0].equals(wordsOfTriplet[2])) {
                            result = "N1=" + count;
                        } else if (grams[0].equals(wordsOfTriplet[1])) {
                            result = "C1=" + count;
                        }
                    } else if (grams.length == 2) {
                        if (grams[0].equals(wordsOfTriplet[1]) && grams[1].equals(wordsOfTriplet[2])) {
                            result = "N2=" + count;
                        } else if (grams[0].equals(wordsOfTriplet[0]) && grams[1].equals(wordsOfTriplet[1])) {
                            result = "C2=" + count;
                        }
                    }
            
                    // Replace commas in triplet with spaces for output
                    String formattedTriplet = tripletContent.replace(",", " ");
                    context.write(new Text(formattedTriplet), new Text(result));
                }
            }
        }

        public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
            private double N1, N2, N3, C0, C1, C2;
            public void initialize(Context context) {
                N1 = -1;
                N2 = -1;
                N3 = -1;
                C0 = -1;
                C1 = -1;
                C2 = -1;
            }

            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                for (Text value : values) {
                    String[] tokens = value.toString().split("=");
                    long val = Long.parseLong(tokens[1]);
                    if(tokens[0].equals("N1"))
                        N1 = val;
                    else if(tokens[0].equals("N2"))
                        N2 = val;
                    else if(tokens[0].equals("N3"))
                        N3 = val;
                    else if (tokens[0].equals("C0"))
                        C0 = val;
                    else if(tokens[0].equals("C1"))
                        C1 = val;
                    else if(tokens[0].equals("C2"))
                        C2 = val;
                }

                if(N1 != -1 && N2 != -1 && N3 != -1 && C0 != -1 && C1 != -1 && C2 != -1){
                    double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
                    double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);

                    double probability = (k3 * N3 / C2) + ((1 - k3) * k2 * N2 / C1) + ((1 - k3) * (1 - k2) * N1 / C0);
                    context.write(key, new DoubleWritable(probability));
                }
                
            }
        }

        public static class PartitionerClass extends Partitioner<Text, Text> {
            public int getPartition(Text key, Text value, int numReduceTasks) {
                return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
            }
        }

            public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        //conf.set("StopWords", stopWords); // Pass stop words to Configuration

        Job job = Job.getInstance(conf, "Calculate Probability");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(Combiner.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set the input and output paths for your local files
        FileInputFormat.addInputPath(job, new Path("s3://ameen-raya-project/" + args[1] + "/out_step1/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://ameen-raya-project/" + args[1] + "/out_step2"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
