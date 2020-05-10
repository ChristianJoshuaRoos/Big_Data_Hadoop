
package yelp.bayesian;

import yelp.tuples.WordStarsTuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

/**
 * @author Christian Roos
 *
 * WordStarsCounter
 * Count of the number of occurrences of each word for each star value. 
 */
public class WordStarsCounter {
    /**
     *  WordStarsMapper
     */
    public static class WordStarsMapper extends Mapper<Object, Text, WordStarsTuple, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private WordStarsTuple tuple = new WordStarsTuple();
        private List<String> stopwords = new ArrayList<String>();

        public void loadStopwords() throws IOException {
            stopwords = Files.readAllLines(Paths.get("~/stopwords.txt"));
        }

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
                System.out.println(cleaned.length() + ": " + cleaned);
            }
            // Get Label
            Integer label = obj == null ? -1 : obj.getInt("stars");
            tuple.stars = label;
            
            // Get Review Text
            String review = obj == null ? "" : obj.getString("text").toLowerCase();
            
            // Get Words
            Pattern pattern = Pattern.compile("\\b[\\w']+\\b");
            Matcher matcher = pattern.matcher(review);
            while (matcher.find()) {
                tuple.word = review.substring(matcher.start(), matcher.end());
                if (!stopwords.contains(tuple.word)) {
                    context.write(tuple, one);
                }
            }
        }
    }

    /**
     *  WordStarsReducer
     */
    public static class WordStarsReducer extends Reducer<WordStarsTuple, IntWritable, WordStarsTuple, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(WordStarsTuple key, Iterable<IntWritable> values, Context context) 
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
  
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path("/output/wordstars_count"), true);
      Job job = new Job(conf, "Label count for each word.");
      job.setJarByClass(WordStarsCounter.class);
      job.setMapperClass(WordStarsMapper.class);
      job.setReducerClass(WordStarsReducer.class);
      job.setOutputKeyClass(WordStarsTuple.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path("/input/review_1000.json"));
      FileOutputFormat.setOutputPath(job, new Path("/output/wordstars_count"));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
