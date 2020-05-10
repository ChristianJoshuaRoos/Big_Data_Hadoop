
package yelp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class ClassCounter {
    /**
     *  CountLabels
     *  Global count of the occurrence of each label in the corpus.
     *  NOTE: Sum of counts is the total number of entries in the corpus.
     */
    public static class LabelMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private static IntWritable label = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            // Initial cleaning of the JSON file line.
            String cleaned = value.toString().replace("\\\\", "\\");
            
            // Try to create a JSON object, or fail "gracefully".
            JSONObject obj = null;
            try {
                obj = new JSONObject(cleaned);
            } catch (Exception e) {
                //System.out.println(e + ":" + cleaned);
                System.out.println(cleaned.length() + ": " + cleaned);
            }
            
            // Get the label from the json object.
            label.set(obj == null ? -1 : obj.getInt("stars"));
            
            // Map
            context.write(label, one);
        }
    }

    /**
     * IntSumReducer
     */
    public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
  
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/output/class_count"), true);
        Job job = new Job(conf, "Count the number of occurrences of the labels.");
        job.setJarByClass(ClassCounter.class);
        job.setMapperClass(LabelMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/input/review_1000.json"));
        FileOutputFormat.setOutputPath(job, new Path("/output/class_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}