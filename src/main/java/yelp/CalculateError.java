
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
import yelp.bayesian.ClassifyBayesian;


public class CalculateError {
    
    public static class ErrorMapper extends Mapper<Object, Text, Text, Text> {        
        private final static Text correct = new Text("correct");
        private final static Text incorrect = new Text("incorrect");
        private final static IntWritable one = new IntWritable(1);
        
        @Override
        public void map(Object key, Text value, Mapper.Context context) 
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            Integer actual = Integer.parseInt(fields[1]);
            Integer predicted = Integer.parseInt(fields[2]);
            
            // Write out the results
            if (actual.equals(predicted)) {
                context.write(correct, one);
            } else{
                context.write(incorrect, one);
            }
        }
    }
    
    public static class ErrorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            // Aggregate the total for each WordStar tuple.
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/output/bayesian_accuracy"), true);
        fs.delete(new Path("/output/bernoulli_accuracy"), true);

        Job job = new Job(conf, "Determine accuracy of the model.");
        job.setJarByClass(CalculateError.class);
        job.setMapperClass(CalculateError.ErrorMapper.class);
        job.setReducerClass(CalculateError.ErrorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/output/bayesian_class"));
        //FileInputFormat.addInputPath(job, new Path("/output/bernoulli_class"));
        FileOutputFormat.setOutputPath(job, new Path("/output/bayesian_accuracy"));
        //FileOutputFormat.setOutputPath(job, new Path("/output/bernoulli_accuracy"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
