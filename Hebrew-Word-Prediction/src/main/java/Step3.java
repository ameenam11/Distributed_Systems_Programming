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

public class Step3 {
    public static class MapperClass extends Mapper<Text, DoubleWritable, Text, Text> {
        @Override
        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString() + ":" + value.get()), new Text());
        }
    }

    // public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
    //     @Override
    //     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //         String line = value.toString();
    //         String[] parts = line.split("\t");
    //         String trigram = parts[0].trim(); // w1 w2 w3
    //         double probability = Double.parseDouble(parts[1].trim()); // P(w3|w1,w2)
    
    //         // Emit trigram as key and probability as value
    //         context.write(new Text(trigram + ":" + probability), new Text(""));
    //     }
    // }
    
    


    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split(":");
            String trigram = parts[0];
            double probability = Double.parseDouble(parts[1]);
    
            context.write(new Text(trigram), new DoubleWritable(probability));
        }
    }

        public static class TrigramPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(Text.class, true);
        }
    
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String[] parts1 = w1.toString().split(":");
            String[] parts2 = w2.toString().split(":");

            String[] p1 = parts1[0].split(" ");
            String[] p2 = parts2[0].split(" ");
    
            String w1w2_1 = p1[0] + " " + p1[1];
            String w1w2_2 = p2[0] + " " + p2[1];
            int cmp = w1w2_1.compareTo(w1w2_2);
            if (cmp != 0) return cmp;
    
            double prob1 = Double.parseDouble(parts1[1]);
            double prob2 = Double.parseDouble(parts2[1]);
            return Double.compare(prob2, prob1); // Descending order
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort trigrams");
    
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
    
        job.setPartitionerClass(TrigramPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
    
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://ameen-raya-project/" + args[1] + "/out_step2/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://ameen-raya-project/" + args[1] + "/out_step3"));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
