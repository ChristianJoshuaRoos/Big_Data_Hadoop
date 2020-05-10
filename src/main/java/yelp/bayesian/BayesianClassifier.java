
package yelp.bayesian;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BayesianClassifier {
    public HashMap<Integer, Integer> classFrequencies;
    public HashMap<Integer, Double> classProbabilities;
    public HashMap<String, HashMap<Integer, Double>> priorProbabilities;
    //public HashMap<String, HashMap<Integer, Integer>> frequencies;
    //public HashMap<String, Double> chiSquares;
    
    public BayesianClassifier(InputStream classCountFile, InputStream wordClassCountFile) {
        this.classFrequencies = generateClassFrequencies(classCountFile);
        this.classProbabilities = generateClassProbabilities(this.classFrequencies);
        this.priorProbabilities = generatePriorProbabilities(wordClassCountFile);
        smooth(this.priorProbabilities);
        //this.frequencies = generateFrequencies(wordClassCountFile);
    }
    
    private final HashMap<Integer, Integer> generateClassFrequencies(InputStream classCountFile) {
        BufferedReader classCountReader = new BufferedReader(new InputStreamReader(classCountFile));
        HashMap<Integer, Integer> classFreq = new HashMap<Integer, Integer>();
        try {
            // 1.   Extract the data
            //      Sample line:        stars   count
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
    
    private final HashMap<Integer,Double> generateClassProbabilities(HashMap<Integer,Integer> classFreq) {
        HashMap<Integer, Double> classProbs = new HashMap<Integer, Double>();
        
        // 1.  Get total number of documents (removed -1 class)
        int total = 0;
        for (Integer c : classFreq.keySet()) {
            if (c != -1) {
                total += classFreq.get(c);
            }
        }
        
        // 2.  Calculate and store class Probability. (removed -1 class)
        for (Integer c : classFreq.keySet()) {
            if (c != -1) {
                classProbs.put(c, classFreq.get(c) / (double)total );
            }
        }
        return classProbs;
    }
    
    private final HashMap<String, HashMap<Integer, Double>> generatePriorProbabilities(InputStream wordClassProbFile) {
        HashMap<String, HashMap<Integer, Double>> priorProb = new HashMap<String, HashMap<Integer, Double>>();
        BufferedReader wordClassCountReader = new BufferedReader(new InputStreamReader(wordClassProbFile));
        try {
        //SAMPLE LINE: 
        //  stars   word:count:prob,word:count:prob,word:count:prob,...

            String line = wordClassCountReader.readLine();
            while (line != null) {
                String[] values = line.split("\t");
                Integer stars = Integer.parseInt(values[0]);
                
                String[] values2 = values[1].split(",");
                for (String values21 : values2) {
                    String[] values3 = values21.split(":");
                    String word = values3[0];
                    Double prob = Double.parseDouble(values3[2]);
                    
                    if (priorProb.containsKey(word)) {
                        HashMap<Integer, Double> starsProb = priorProb.get(word);
                        starsProb.put(stars, prob);
                    } else {
                        HashMap<Integer, Double> starsProb = new HashMap<Integer, Double>();
                        starsProb.put(stars, prob);
                        priorProb.put(word, starsProb);
                    }
                }                
                
                line = wordClassCountReader.readLine();
            }
        } catch (IOException e) {
            System.out.println("Error creating word stars probabilities.");
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
    
    /**
    private HashMap<String, HashMap<Integer, Integer>> generateFrequencies(InputStream wordClassProbFile) {
        HashMap<String, HashMap<Integer, Integer>> freq = new HashMap<String, HashMap<Integer, Integer>>();
        BufferedReader wordClassCountReader = new BufferedReader(new InputStreamReader(wordClassProbFile));
        try {
        //SAMPLE LINE: old	1:1:0.14285714285714285,2:1:0.14285714285714285,4:2:0.2857142857142857,5:3:0.42857142857142855,

            String line = wordClassCountReader.readLine();
            while (line != null) {
                String[] values = line.split("\t");
                String word = values[0];
                
                HashMap<Integer, Integer> starsFrequency = new HashMap<Integer, Integer>(); 
                String[] values2 = values[1].split(",");
                for (String values21 : values2) {
                    String[] values3 = values21.split(":");
                    Integer stars = Integer.parseInt(values3[0]);
                    Integer frequency = Integer.parseInt(values3[1]);
                    starsFrequency.put(stars, frequency);
                }                
                
                freq.put(word, starsFrequency);
                line = wordClassCountReader.readLine();
            }
        } catch (IOException e) {
            System.out.println("Error creating word stars frequencies.");
        }
        return freq;
    }
    */
    /**
    public HashMap<String, Double> generateChiSquare() {
        HashMap<String, Double> wordChiSquare = new HashMap<String, Double>();
        for (String word: this.frequencies.keySet()){
            
        }
        
        return wordChiSquare;
    }
    */

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream classCountFile = 
                fs.open(new Path("/output/class_count/part-r-00000"));
        FSDataInputStream wordClassProbFile = 
                fs.open(new Path("/output/starsword_prob/part-r-00000"));
        
        BayesianClassifier model = new BayesianClassifier(classCountFile, wordClassProbFile);
        System.out.println(model.classProbabilities);
        System.out.println(model.priorProbabilities);
    }
    
}
