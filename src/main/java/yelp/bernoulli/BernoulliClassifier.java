
package yelp.bernoulli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BernoulliClassifier {
    public HashMap<Integer, Integer> classFrequencies;
    public HashMap<Integer, Double> classProbabilities;
    public HashMap<Integer, HashMap<String, Integer>> classWordFrequencies;
    public HashMap<String, HashMap<Integer, Double>> priorProbabilities;
    
    public BernoulliClassifier(InputStream classCountFile, InputStream wordClassCountFile) {
        this.classFrequencies = generateClassFrequencies(classCountFile);
        this.classProbabilities = generateClassProbabilities(this.classFrequencies);
        this.classWordFrequencies = generateClassWordFrequencies(wordClassCountFile);
        this.priorProbabilities = generatePriorProbabilities(this.classWordFrequencies);
        smooth(this.priorProbabilities);
    }
    
    private final HashMap<Integer,Integer> generateClassFrequencies(InputStream classCountFile) {
        BufferedReader classCountReader = new BufferedReader(new InputStreamReader(classCountFile));
        HashMap<Integer, Integer> classFreq = new HashMap<Integer, Integer>();
        // Sample Line:
        // Stars    Count

        // 1.  Extract data.
        try {
            String line = classCountReader.readLine();
            while ( line != null ){
                String[] values = line.split("\t");
                Integer stars = Integer.parseInt(values[0]);
                Integer count = Integer.parseInt(values[1]);
                classFreq.put(stars, count);
                line = classCountReader.readLine();
            }
        } catch (IOException e) {
            System.out.println("Error creating class frequencies from class count file.");
        }
        
        return classFreq;
    }
    
    private final HashMap<Integer,Double> generateClassProbabilities(HashMap<Integer, Integer> classFreq) {
        HashMap<Integer, Double> classProbs = new HashMap<Integer, Double>();
        
        // 1.  Get total number of documents (removed -1 class)
        int total = 0;
        for (Integer c : classFreq.keySet()) {
            if (c != -1){
                total += classFreq.get(c);
            }
        }
        
        // 2.  Calculate and store class probability P(C).  (removed -1 class)
        for (Integer c : classFreq.keySet()) {
            if (c != -1) {
                classProbs.put(c, classFreq.get(c) / (double)total );
            }
        }
        return classProbs;
    }
    
    private final HashMap<Integer, HashMap<String, Integer>> generateClassWordFrequencies(InputStream wordClassProbFile) {
        HashMap<Integer, HashMap<String, Integer>> classWordFreq = new HashMap<Integer, HashMap<String, Integer>>();
        BufferedReader wordClassCountReader = new BufferedReader(new InputStreamReader(wordClassProbFile));
        //SAMPLE LINE: 
        //  stars   word:count,word:count,word:count
        
        // 1.  Extract data.
        try {
            String line = wordClassCountReader.readLine();
            while (line != null) {
                
                String[] values = line.split("\t");
                Integer stars = Integer.parseInt(values[0]);
                
                HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
                String[] values2 = values[1].split(",");
                for (String tuple: values2) {
                    String[] values3 = tuple.split(":");
                    wordCount.put(values3[0], Integer.parseInt(values3[1]));
                }
                
                classWordFreq.put(stars, wordCount);
                line = wordClassCountReader.readLine();
            }
            
        } catch (IOException e) {
            System.out.println("Error creating word stars probabilities.");
        }

        return classWordFreq;
    }
    
    private final HashMap<String, HashMap<Integer, Double>>  generatePriorProbabilities(HashMap<Integer, HashMap<String, Integer>> classWordFreq) {
        HashMap<String, HashMap<Integer, Double>> priorProb = new HashMap<String, HashMap<Integer, Double>>();
        
        for (Integer stars: classWordFreq.keySet()) {
            HashMap<String, Integer> wordCount = classWordFreq.get(stars);
            for (String word: wordCount.keySet()) {
                Integer count = wordCount.get(word);
                Double prob = (count + 1)/ ((double)this.classFrequencies.get(stars) + 2);
                
                if (priorProb.containsKey(word)) {
                    HashMap<Integer, Double> starsProb = priorProb.get(word);
                    starsProb.put(stars,prob);
                } else {
                    HashMap<Integer, Double> starsProb = new HashMap<Integer, Double>();
                    starsProb.put(stars, prob);
                    priorProb.put(word, starsProb);
                }
                
            }
        }
        return priorProb;
    }
    
    public void smooth(HashMap<String, HashMap<Integer, Double>> priorProb) {
        for (String word: priorProb.keySet()) {
            HashMap<Integer, Double> classProbs = priorProb.get(word);
            for (Integer i = 1; i <= 5; i++) {
                if (!classProbs.containsKey(i)) {
                    classProbs.put(i, (1.0 / ((double)this.classFrequencies.get(i) + 2)));
                }
            }
        }
    }
    
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream classCountFile = 
                fs.open(new Path("/output/class_count/part-r-00000"));
        FSDataInputStream wordClassProbFile = 
                fs.open(new Path("/output/starsdocs_count/part-r-00000"));
        
        BernoulliClassifier model = new BernoulliClassifier(classCountFile, wordClassProbFile);
        System.out.println(model.classProbabilities);
        System.out.println(model.priorProbabilities);
    }

}
