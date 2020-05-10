/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelp.bernoulli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
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

import static java.nio.file.Files.*;
import static java.nio.file.Files.readAllLines;

/**
 * @author Christian Roos
 */
public class StarsDocsCounter {
    
    public static class StarsDocsMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final IntWritable stars = new IntWritable();
        private final Text word = new Text();
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
            String review = obj == null ? "" : obj.getString("text").toLowerCase();
            Integer c = obj == null ? -1 : obj.getInt("stars");
            
            // Get words
            Pattern pattern = Pattern.compile("\\b[\\w']+\\b");
            Matcher matcher = pattern.matcher(review);
            Set<String> words = new HashSet<String>();  // Removes duplicates!
            while (matcher.find()) {
                words.add(review.substring(matcher.start(), matcher.end()));
            }

            //Removes stop-words from set of all words.
            for (String word : words){
                if (stopwords.contains(word)){
                    words.remove(word);
                }
            }
            
            // Create the class
            stars.set(c);
            
            // Add word only once per document.
            for (String w: words) {
                // Create tuple of word and count.
                word.set(w);
                context.write(stars, word);
            }
        }
    }

    /**
     * StarsDocsReducer
     */
    public static class StarsDocsReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
        private Text results = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            // 1.  Receive : 
            //      stars  word,word,word,word...
            HashMap<String, Integer> wordCount = new HashMap<String,Integer>();
            for (Text val : values) {
                String word = val.toString();
                if (wordCount.containsKey(word)) {
                    Integer current = wordCount.get(word);
                    wordCount.put(word, current + 1);
                } else {
                    wordCount.put(word, 1);
                }
            }
            
            // 2.  Output: 
            //      stars    word:count,word:count,word:count....
            StringBuilder sb = new StringBuilder();
            for (String word : wordCount.keySet()) {
                sb.append(word).append(":").append(wordCount.get(word)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            results.set(sb.toString());
            context.write(key, results);
        }
    }
  
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/output/starsdocs_count"), true);
        Job job = new Job(conf, "Count the number of documents in a class.");
        job.setJarByClass(StarsDocsCounter.class);
        job.setMapperClass(StarsDocsMapper.class);
        job.setReducerClass(StarsDocsReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/input/review_1000.json"));
        FileOutputFormat.setOutputPath(job, new Path("/output/starsdocs_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
