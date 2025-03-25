import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.io.IOException;
import java.util.HashSet;

public class Step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final String stopWordsString = "״\n" +
        "׳\n" +
        "של\n" +
        "רב\n" +
        "פי\n" +
        "עם\n" +
        "עליו\n" +
        "עליהם\n" +
        "על\n" +
        "עד\n" +
        "מן\n" +
        "מכל\n" +
        "מי\n" +
        "מהם\n" +
        "מה\n" +
        "מ\n" +
        "למה\n" +
        "לכל\n" +
        "לי\n" +
        "לו\n" +
        "להיות\n" +
        "לה\n" +
        "לא\n" +
        "כן\n" +
        "כמה\n" +
        "כלי\n" +
        "כל\n" +
        "כי\n" +
        "יש\n" +
        "ימים\n" +
        "יותר\n" +
        "יד\n" +
        "י\n" +
        "זה\n" +
        "ז\n" +
        "ועל\n" +
        "ומי\n" +
        "ולא\n" +
        "וכן\n" +
        "וכל\n" +
        "והיא\n" +
        "והוא\n" +
        "ואם\n" +
        "ו\n" +
        "הרבה\n" +
        "הנה\n" +
        "היו\n" +
        "היה\n" +
        "היא\n" +
        "הזה\n" +
        "הוא\n" +
        "דבר\n" +
        "ד\n" +
        "ג\n" +
        "בני\n" +
        "בכל\n" +
        "בו\n" +
        "בה\n" +
        "בא\n" +
        "את\n" +
        "אשר\n" +
        "אם\n" +
        "אלה\n" +
        "אל\n" +
        "אך\n" +
        "איש\n" +
        "אין\n" +
        "אחת\n" +
        "אחר\n" +
        "אחד\n" +
        "אז\n" +
        "אותו\n" +
        "־\n" +
        "^\n" +
        "?\n" +
        ";\n" +
        ":\n" +
        "1\n" +
        ".\n" +
        "-\n" +
        "*\n" +
        "\"\n" +
        "!\n" +
        "שלשה\n" +
        "בעל\n" +
        "פני\n" +
        ")\n" +
        "גדול\n" +
        "שם\n" +
        "עלי\n" +
        "עולם\n" +
        "מקום\n" +
        "לעולם\n" +
        "לנו\n" +
        "להם\n" +
        "ישראל\n" +
        "יודע\n" +
        "זאת\n" +
        "השמים\n" +
        "הזאת\n" +
        "הדברים\n" +
        "הדבר\n" +
        "הבית\n" +
        "האמת\n" +
        "דברי\n" +
        "במקום\n" +
        "בהם\n" +
        "אמרו\n" +
        "אינם\n" +
        "אחרי\n" +
        "אותם\n" +
        "אדם\n" +
        "(\n" +
        "חלק\n" +
        "שני\n" +
        "שכל\n" +
        "שאר\n" +
        "ש\n" +
        "ר\n" +
        "פעמים\n" +
        "נעשה\n" +
        "ן\n" +
        "ממנו\n" +
        "מלא\n" +
        "מזה\n" +
        "ם\n" +
        "לפי\n" +
        "ל\n" +
        "כמו\n" +
        "כבר\n" +
        "כ\n" +
        "זו\n" +
        "ומה\n" +
        "ולכל\n" +
        "ובין\n" +
        "ואין\n" +
        "הן\n" +
        "היתה\n" +
        "הא\n" +
        "ה\n" +
        "בל\n" +
        "בין\n" +
        "בזה\n" +
        "ב\n" +
        "אף\n" +
        "אי\n" +
        "אותה\n" +
        "או\n" +
        "אבל\n" +
        "א\n";

        private HashSet<String> stopWords = createHashSetStopWords();

        private HashSet<String> createHashSetStopWords(){
            HashSet<String> newHash = new HashSet<>();
            String[] stopWordsArray = stopWordsString.split("\n");
            for (String word: stopWordsArray) {
                newHash.add(word);
            }
            return newHash;
        }

        private Text ngram = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 4) {
                return; // Ensure input line has exact 4 tokens
            }
            String gram = tokens[0], count = tokens[2];

            String[] words = gram.split("\\s+");
            if(words.length != 3) {
                return; // Ensure ngram has 3 words
            }

            String w1 = words[0], w2 = words[1], w3 = words[2];

            if(containsStopWord(w1, w2, w3))
                return; // Skip stop words

            context.write(new Text(w2 + ":*"), new Text(count));
            ngram.set(w2);
            context.write(ngram, new Text("(" + w1 + "," + w2 + "," + w3 + ")"));

            context.write(new Text(w3 + ":*"), new Text(count));
            ngram.set(w3);
            context.write(ngram, new Text("(" + w1 + "," + w2 + "," + w3 + ")"));

            context.write(new Text(w1 + " " + w2 + ":*"), new Text(count));
            ngram.set(w1 + " " + w2);
            context.write(ngram, new Text("(" + w1 + "," + w2 + "," + w3 + ")"));

            context.write(new Text(w2 + " " + w3 + ":*"), new Text(count));
            ngram.set(w2 + " " + w3);
            context.write(ngram, new Text("(" + w1 + "," + w2 + "," + w3 + ")"));

            ngram.set(w1 + " " + w2 + " " + w3);
            context.write(ngram, new Text(count));
            
            context.write(new Text("C0:*"), new Text(String.valueOf(Integer.parseInt(count) * 3) + ""));
            ngram.set("C0");
            context.write(ngram, new Text("(" + w1 + "," + w2 + "," + w3 + ")"));
        }

        private boolean containsStopWord(String w1, String w2, String w3){
            return stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3);
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();
            if (keyString.equals("C0:*")) {
                long sum = 0;
                for (Text value : values) {
                    sum += Long.parseLong(value.toString());
                }
                context.write(key, new Text(sum + ""));

            }else if(keyString.equals("C0")){
                for (Text value : values) {
                    context.write(key, value);
                }
            }else if (keyString.endsWith(":*")) {
                long partialSum = 0;
                for (Text value : values) {
                    partialSum += Long.parseLong(value.toString());
                }
                context.write(key, new Text(partialSum + ""));
            }else {
                for (Text value : values) {
                    context.write(key, value);
                }
            }
        }
    }
    

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private long lastSum = 0;
    
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();
            
            if(keyString.equals("C0:*")){
                long sum = 0;
                for (Text value : values) {
                    int count = Integer.parseInt(value.toString());
                    sum += count;
                }
                lastSum = sum;
            }else if(keyString.equals("C0")){
                for (Text value : values) {
                    String triplet = value.toString();
                    context.write(key, new Text(lastSum + ":" + triplet));
                }
                lastSum = 0;
            }else{
                if (keyString.endsWith(":*")) {
                    long sum = 0;
                    for (Text value : values) { // (ngram:*, count)
                        int count = Integer.parseInt(value.toString());
                        sum += count;
                    }
                    lastSum = sum;
                } else {
                    for (Text value : values) { // (1gram, triplet) or (2gram, triplet) or (3gram, "")
                        String[] keyArray = keyString.split(" ");
                        if (keyArray.length == 3) {
                            context.write(key, new Text(lastSum + ""));
                        } else {
                            String triplet = value.toString();
                            context.write(key, new Text(lastSum + ":" + triplet));
                        }
                    }
                    lastSum = 0;
                }
            }
        }
    }

    public static class NGramPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String normalizedKey = key.toString().replace(":*", "");        
            return (normalizedKey.hashCode() & Integer.MAX_VALUE) % numPartitions ;
        }
    }

        public static class KeyComparator extends WritableComparator {
            public KeyComparator() {
                super(Text.class, true);
            }
        
            @Override
            public int compare(WritableComparable w1, WritableComparable w2) {
                Text key1 = (Text) w1;
                Text key2 = (Text) w2;
        
                boolean hasStar1 = key1.toString().endsWith(":*");
                boolean hasStar2 = key2.toString().endsWith(":*");
        
                String cleanKey1 = key1.toString().replace(":*", "");
                String cleanKey2 = key2.toString().replace(":*", "");
        
                String[] words1 = cleanKey1.split(" ");
                String[] words2 = cleanKey2.split(" ");
        
                int cmp = Integer.compare(words1.length, words2.length);
                if (cmp != 0) return cmp;
        
                for (int i = 0; i < Math.min(words1.length, words2.length); i++) {
                    cmp = words1[i].compareTo(words2[i]);
                    if (cmp != 0) return cmp;
                }
        
                if (hasStar1 && !hasStar2) return -1; // ngram:* comes first
                if (!hasStar1 && hasStar2) return 1;  // ngram comes second
        
                return 0;
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
            job.setSortComparatorClass(KeyComparator.class);
            job.setCombinerClass(CombinerClass.class);
            job.setReducerClass(ReducerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            // For n_grams S3 files.
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
            FileOutputFormat.setOutputPath(job, new Path("s3://ameen-raya-project/" + args[1] + "/out_step1"));

            // FileInputFormat.addInputPath(job, new Path("s3://ameen-raya-project/arbix.txt"));
            // FileOutputFormat.setOutputPath(job, new Path("s3://ameen-raya-project/out_step1"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}