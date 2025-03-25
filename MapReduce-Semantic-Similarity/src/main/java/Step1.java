import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.IdentityReducer;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private static Set<String> relatedLexemes = new HashSet<>();

    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
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
    }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Stemmer stemmer = new Stemmer();
            String line = value.toString();
            String[] tokens = line.split("\t");
        
            if (tokens.length < 3) return; // Ensure minimum required fields exist
        
            String[] ngram = tokens[1].split(" ");
            String total_count = tokens[2];
            Set<String> uniqueFeatures = new HashSet<>();
        
            for (String token : ngram) {
                String[] parts = token.split("/");
        
                if (parts.length < 4) {
                    System.err.println("Skipping malformed token: " + token);
                    continue; 
                }
        
                String lexeme = parts[0];
                String dep = parts[2];
                String headIndexStr = parts[3].trim();
                int head_index = 0;
        
                try {
                    head_index = Integer.parseInt(headIndexStr);
                } catch (NumberFormatException e) {
                    continue; 
                }
            

                if(head_index == 0) {
                    continue;
                }

                stemmer.add(lexeme.toCharArray(), lexeme.length());
                stemmer.stem();
                String stemmedLexeme = stemmer.toString();
                String featureLexeme = "";
                try {
                    featureLexeme = ngram[head_index - 1].split("/")[0];
                } catch (Exception e) {
                    System.err.println("ngram" + ngram);
                    System.err.println("Skipping malformed token: " + token);
                    continue;
                }
                
                stemmer.add(featureLexeme.toCharArray(), featureLexeme.length());
                stemmer.stem();
                String stemmedFeatureLexeme = stemmer.toString();

                String feature = stemmedFeatureLexeme + "-" + dep;
                uniqueFeatures.add(feature);

                if (relatedLexemes.contains(stemmedLexeme)) {
                    // Emit the pair ((stemmedLexeme, feature), total_count)
                    String pair = stemmedLexeme + "," + feature;
                    context.write(new Text(pair), new Text(total_count));

                    // Emit the pair (stemmedLexeme, total_count)
                    context.write(new Text("LEX:" + stemmedLexeme), new Text(total_count));
                }

                // Emit the pair (feature, total_count)
                context.write(new Text("FEAT:" + feature), new Text(total_count));
            }
            // Emit the pair ("L*", n * total_count)
            context.write(new Text("L*"), new Text(((ngram.length - 1) * Integer.parseInt(total_count)) + ""));

            // Emit the pair ("F*", n * total_count)
            context.write(new Text("F*"), new Text((uniqueFeatures.size() * Integer.parseInt(total_count)) + ""));
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (Text value : values) {
                sum += Long.parseLong(value.toString());
            }

            context.write(key, new Text(sum + ""));
        }
    }

    

    public static class ReducerClass extends Reducer<Text, Text, Text, LongWritable> {
        private long LStar = 0;
        private long FStar = 0;
        private List<Map.Entry<String, Long>> featureList = new ArrayList<>();
        private List<Map.Entry<String, Long>> lexemeList = new ArrayList<>();
        //todo add the S3 paths as fields

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            String keyStr = key.toString();

            for (Text value : values) {
                sum += Long.parseLong(value.toString());
            }

            if (keyStr.startsWith("L*")) {
                LStar += sum;
            } else if (keyStr.startsWith("F*")) {
                FStar += sum;
            } else if (keyStr.startsWith("LEX:")) {
                lexemeList.add(new AbstractMap.SimpleEntry<>(keyStr.substring(4), sum));
            } else if (keyStr.startsWith("FEAT:")) {
                featureList.add(new AbstractMap.SimpleEntry<>(keyStr.substring(5), sum));
            } else {
                context.write(key, new LongWritable(sum));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
        
            FileSystem fs = FileSystem.get(URI.create("s3a://ameen-raya-hw3/"), conf);
        
            Path outputPath = new Path("s3a://ameen-raya-hw3/global_counts.txt");
            StringBuilder content = new StringBuilder();
            if (fs.exists(outputPath)) {
                try (FSDataInputStream in = fs.open(outputPath);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        content.append(line).append("\n");
                    }
                }
            }

            // Add new data
            if(LStar != 0)
                content.append("L*").append("\t").append(LStar).append("\n");
            if(FStar != 0)
                content.append("F*").append("\t").append(FStar).append("\n");

            // Overwrite the file with new content
            if(LStar != 0 || FStar != 0){
                try (FSDataOutputStream out = fs.create(outputPath, true)) { 
                    out.writeBytes(content.toString());
                }
            }
        
            featureList.sort((o1, o2) -> Long.compare(o2.getValue(), o1.getValue()));
        
            int startRank = Math.min(100, featureList.size());
            int endRank = Math.min(1100, featureList.size());
        
            Path featuresPath = new Path("s3a://ameen-raya-hw3/features_count.txt");
            if(featureList.size() > 0) {
                try (FSDataOutputStream out = fs.create(featuresPath, true)) { 
                    for (int i = startRank; i < endRank; i++) {
                        Map.Entry<String, Long> entry = featureList.get(i);
                        out.writeBytes(entry.getKey() + "\t" + entry.getValue() + "\n");
                    }
                }
            }

            if(lexemeList.size() > 0) {
                Path lexemesPath = new Path("s3a://ameen-raya-hw3/lexemes_count.txt");
                try (FSDataOutputStream out = fs.create(lexemesPath, true)) { 
                    for (Map.Entry<String, Long> entry : lexemeList) {
                        out.writeBytes(entry.getKey() + "\t" + entry.getValue() + "\n");
                    }
                }
            }
        }
        
    }

        public static class NGramPartitioner extends Partitioner<Text, Text> {
            @Override
            public int getPartition(Text key, Text value, int numPartitions) {
                String keyStr = key.toString();
                if (keyStr.startsWith("LEX:")) {
                    return 0; // All "LEX:" keys go to reducer 0
                } else if (keyStr.startsWith("FEAT:")) {
                    return 1 % numPartitions; // All "FEAT:" keys go to reducer 1
                } else {
                    return (keyStr.hashCode() & Integer.MAX_VALUE) % numPartitions;
                }
            }
        }
        
        

        public static void main(String[] args) throws Exception {
            System.out.println("[DEBUG] STEP 1 started!");
            System.out.println(args.length > 0 ? args[0] : "no args");
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Calculate Parameters");
            job.setJarByClass(Step1.class);
            job.setMapperClass(MapperClass.class);
            job.setPartitionerClass(NGramPartitioner.class);
            // job.setSortComparatorClass(KeyComparator.class);
            job.setCombinerClass(CombinerClass.class);  // with combiner
            job.setReducerClass(ReducerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            
            int i = 0;
            // while(i < args.length){
            //     FileInputFormat.addInputPath(job, new Path(args[i]));
            //     i++;
            // }

            while(i < 9){
                FileInputFormat.addInputPath(job, new Path("s3://biarcs/" + i + ".txt"));
                i++;
            }

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            // FileInputFormat.addInputPath(job, new Path("s3://ameen-raya-hw3/biarcs.00-of-99"));
            FileOutputFormat.setOutputPath(job, new Path("s3://ameen-raya-hw3/out_step1"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}