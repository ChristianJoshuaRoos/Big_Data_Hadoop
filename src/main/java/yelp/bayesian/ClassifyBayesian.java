package yelp.bayesian;

import yelp.bayesian.BayesianClassifier;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

/**
 * @author Christian Roos
 */
public class ClassifyBayesian {
 
    public static class ClassifyMapper extends Mapper<Object, Text, Text, Text> {
        private BayesianClassifier model;
        
        private final Text id = new Text();
        private final Text result = new Text();
        
        @Override
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            // Load the Bayesian Classifier, if not yet created.  Should only occur
            // once per node.
            if (this.model == null) {
                try {
                    Configuration conf = context.getConfiguration();
                    FileSystem fs = FileSystem.get(conf);
                    FSDataInputStream classCountFile = 
                            fs.open(new Path("/output/class_count/part-r-00000"));
                    FSDataInputStream wordClassCountFile = 
                            fs.open(new Path("/output/starsword_prob/part-r-00000"));
                    this.model = new BayesianClassifier(classCountFile, wordClassCountFile);
                } catch (IOException e) {
                    System.out.println("ERROR: Unable to create classifier.");
                }
            }

            System.out.println(this.model.classProbabilities);

            if (this.model == null) {
                throw new IOException("Model was not generated correctly.");
            }
            
            if (this.model.classProbabilities == null) {
                    throw new IOException("Class Probabilities were not generated correctly.");
            }
            
            if (this.model.priorProbabilities == null) {
                    throw new IOException("Prior Probabilities were not generated correctly.");
            }
            
            // Load the JSON object.
            // Initial cleaning of the JSON file line.
            String cleaned = value.toString().replace("\\\\", "\\");
            
            // Try to create a JSON object, or fail "gracefully".
            JSONObject obj = null;
            try {
                obj = new JSONObject(cleaned);
            } catch (Exception e) {
                System.out.println(cleaned.length() + ": " + cleaned);
            }
            
            // Get Review ID:
            String reviewID = obj == null ? "error" : obj.getString("review_id");
            this.id.set(reviewID);
            
            // Get Actual Label
            Integer label = obj == null ? -1 : obj.getInt("stars");
            
            // Get the review from the JSON object
            String review = obj == null ? "" : obj.getString("text").toLowerCase();
            Pattern pattern = Pattern.compile("\\b[\\w']+\\b");
            Matcher matcher = pattern.matcher(review);

            List<String> words = new ArrayList<String>();
            while (matcher.find()){
                String word = review.substring(matcher.start(), matcher.end());
                words.add(word);
            }

            // Create a result map
            HashMap<Integer, Double> classificationProbabilities = new HashMap<>();
            // For every possible label.
            for (Integer c = 1; c <= 5; c++) {//CHANGE BACK TO FIVE CLASSES!
                // Get the class probability:
                if (!this.model.classProbabilities.containsKey(c)) {
                    throw new IOException("Class probability not found: " + c);
                }
                Double classProb = Math.log(this.model.classProbabilities.get(c));

                // For each word, add the prior probability for the word having that class of star rating.
                for (String word: words){
                    if (this.model.priorProbabilities.containsKey(word) ) {
                        HashMap<Integer, Double> classProbs = this.model.priorProbabilities.get(word);
                        if (classProbs.containsKey(c)) {
                            classProb += Math.log(classProbs.get(c));
                        }
                    }
                }
                classificationProbabilities.put(c, classProb);
            }
            
            // Get the most probable class.
            Integer maxClass = -1;
            Double maxValue = Double.NEGATIVE_INFINITY;
            for (Integer c : classificationProbabilities.keySet()) {
                if (classificationProbabilities.get(c) > maxValue) {
                    maxClass = c;
                    maxValue = classificationProbabilities.get(c);
                }
            }
            
            this.result.set(label + "\t" + maxClass);
            
            context.write(this.id, this.result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/output/bayesian_class"), true);

        Job job = new Job(conf, "Classify the dataset.");
        job.setJarByClass(ClassifyBayesian.class);
        job.setMapperClass(ClassifyMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/input/review_1000.json"));
        FileOutputFormat.setOutputPath(job, new Path("/output/bayesian_class"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
