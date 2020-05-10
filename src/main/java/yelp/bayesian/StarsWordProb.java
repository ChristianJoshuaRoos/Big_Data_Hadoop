
package yelp.bayesian;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Christian Roos
 */
public class StarsWordProb {
    
    public static class LabelWordMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text stars = new Text();
        private Text tuple = new Text();
        
        @Override
        /**
         *  Transform lines like:
         *      word    stars   count
         *  To:
         *      stars   word:count,word:count,word:count....
         */
        public void map(LongWritable key, Text value, Mapper.Context context) 
                throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t");
            stars.set(fields[1]);
            tuple.set(fields[0] + ":" + fields[2]);
            context.write(stars, tuple);
        }
    }
    
    public static class LabelWordReducer extends Reducer<Text, Text, Text, Text> {
        private Text label = new Text();
        
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) 
                throws IOException, InterruptedException {
            // Sample Line:
            // stars    word:count,word:count,word:count...
            
            // Recreate the data from the values.  
            HashMap<String, Integer> mapCounts = new HashMap<String, Integer>();

            Iterator<Text> itr = value.iterator();
            while(itr.hasNext()) {
                Text text = itr.next();
                String[] values = text.toString().split(":");
                mapCounts.put(values[0], Integer.parseInt(values[1]));
            }
            
            // Calculate the total number of words for the class
            double total = 0.0;
            for (String word : mapCounts.keySet()) {
                total += mapCounts.get(word);
            }
            
            // Calculate the probability of a word given its class.
            HashMap<String, Double> mapProbs = new HashMap<String, Double>();
            for (String word: mapCounts.keySet()) {
                mapProbs.put(word, mapCounts.get(word) / total);
            }
            
            // Create the output value
            StringBuilder sb = new StringBuilder();
            for (String word: mapProbs.keySet()) {
                sb.append(word).append(":")
                  .append(mapCounts.get(word)).append(":")
                  .append(mapProbs.get(word)).append(",");
            }

            label.set(sb.toString());
            context.write(key, label);
            
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/output/starsword_prob"), true);
        Job job = new Job(conf, "Prior probability for each word given its class.");
        job.setJarByClass(StarsWordProb.class);
        job.setMapperClass(StarsWordProb.LabelWordMapper.class);
        job.setReducerClass(StarsWordProb.LabelWordReducer.class);
        job.setOutputKeyClass(Text.class);                  // These are not what they appear to be.
        job.setOutputValueClass(Text.class);                //
        FileInputFormat.addInputPath(job, new Path("/output/wordstars_count/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/output/starsword_prob"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
