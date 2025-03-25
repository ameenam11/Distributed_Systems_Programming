import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class Step2 {
        public static class MapperClass extends Mapper<Text, LongWritable, Text, Text> {
            public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
                String line = key.toString();
                String[] pair = line.split(",");
                String lexeme = pair[0];
                String feature = pair[1];
                long count = value.get();
                context.write(new Text(lexeme), new Text(feature + "," + count));
            }
        }

        public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
 
            private Map<String, Integer> features_count = new HashMap<>();
            private Map<String, Integer> lexemes_count = new HashMap<>();
            private Map<String, Integer> features_index = new HashMap<>();
            private long LStar = 0;
            private long FStar = 0;

            protected void setup(Context context) throws IOException, InterruptedException {
                //load features_count, lexemes_count, LStar and FStar

                /*
                 *  super.setup(context);
                    FileSystem fs = null;
                    BufferedReader br = null;
                    try {
                        Configuration conf = context.getConfiguration();
                        fs = FileSystem.get(new URI("s3a://ameen-raya-hw3/"), conf); // Use s3a
                        Path path = new Path("s3a://ameen-raya-hw3/lexemes.txt"); // Change s3 -> s3a

                        br = new BufferedReader(new InputStreamReader(fs.open(path)));
                        String line;
                        while ((line = br.readLine()) != null) {
                            relatedLexemes.add(line.trim());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (br != null) br.close();
                        if (fs != null) fs.close();
        }
                 */
                super.setup(context);
                FileSystem fs = null;
                BufferedReader br = null;
                try{
                    Configuration conf = context.getConfiguration();
                    fs = FileSystem.get(new URI("s3a://ameen-raya-hw3/"), conf);
                    Path path = new Path("s3a://ameen-raya-hw3/features_count.txt");
                    br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            features_count.put(parts[0], Integer.parseInt(parts[1]));
                            features_index.put(parts[0], features_count.size() - 1);
                        }
                    }
                    path = new Path("s3a://ameen-raya-hw3/lexemes_count.txt");
                    br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            lexemes_count.put(parts[0], Integer.parseInt(parts[1]));
                        }
                    }
                    br.close();
    
                    path = new Path("s3a://ameen-raya-hw3/global_counts.txt");
                    br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    line = br.readLine();
                    LStar = Long.parseLong(line.split("\t")[1]);
                    line = br.readLine();
                    FStar = Long.parseLong(line.split("\t")[1]);
                    br.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) br.close();
                    if (fs != null) fs.close();
                }



            }
            
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                //calculate vector on equation 5
                double[] vector5 = new double[1000], vector6 = new double[1000], vector7 = new double[1000], vector8 = new double[1000]; 

                String lexeme = key.toString();
                long lexeme_count = lexemes_count.containsKey(lexeme) ? lexemes_count.get(lexeme) : -1;
                if(lexeme_count == -1){
                    return;
                }
                StringBuilder output = new StringBuilder();
                
                for (Text value : values) {
                    String[] parts = value.toString().split(",");
                    String feature = parts[0];
                    long count = Long.parseLong(parts[1]);
                    int feature_index = features_index.containsKey(feature) ? features_index.get(feature) : -1;
                    int feature_count = features_count.containsKey(feature) ? features_count.get(feature) : -1;
                    
                    if(feature_index == -1 || feature_count == -1){
                        continue;
                    }
                    //equation 5 --> assoc_freq = count(l,f)
                    if (features_count.containsKey(feature)) {
                        vector5[feature_index] = count;
                    }

                    //equation 6 --> assoc_prob = P(f|l)
                    if(features_count.containsKey(feature)) {
                        vector6[feature_index] = (double) count / lexeme_count;
                    }

                    //equation 7 --> assoc_PMI = PMI(l,f)
                    if(features_count.containsKey(feature)) {
                        double P_lf = (double) count / LStar;
                        double P_f = (double) feature_count / FStar;
                        double P_l = (double) lexeme_count / LStar;

                        vector7[feature_index] = Math.log(P_lf / (P_l * P_f));
                    }

                    //equation 8 --> assoc_t-test = t-test(l,f)
                    if(features_count.containsKey(feature)) {
                        double P_lf = (double) count / LStar;
                        double P_f = (double) feature_count / FStar;
                        double P_l = (double) lexeme_count / LStar;

                        vector8[feature_index] = (P_lf - (P_l * P_f)) / Math.sqrt(P_l * P_f);
                    }
                }

                //write vectors to context
                for (int i = 0; i < 1000; i++) {
                    output.append(vector5[i] + " ");
                }
                context.write(new Text("5," + lexeme), new Text(output.toString()));

                output = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    output.append(vector6[i] + " ");
                }
                context.write(new Text("6," + lexeme), new Text(output.toString()));

                output = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    output.append(vector7[i] + " ");
                }
                context.write(new Text("7," + lexeme), new Text(output.toString()));

                output = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    output.append(vector8[i] + " ");
                }
                context.write(new Text("8," + lexeme), new Text(output.toString()));

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

        Job job = Job.getInstance(conf, "Calculate Probability");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(Combiner.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set the input and output paths for your local files
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://ameen-raya-hw3/out_step1/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://ameen-raya-hw3/out_step2"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
