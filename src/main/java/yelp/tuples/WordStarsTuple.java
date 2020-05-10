/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelp.tuples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Christian Roos
 */
public class WordStarsTuple implements WritableComparable<WordStarsTuple> {

    public String word;
    public Integer stars;
    
    public WordStarsTuple() {
    }
    
    public WordStarsTuple(String word, Integer stars) {
        this.word = word;
        this.stars = stars;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.word);
        out.writeInt(this.stars);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.word = in.readUTF();
        this.stars = in.readInt();
    }

    @Override
    public int compareTo(WordStarsTuple o) {
        if (this.word.equals(o.word)) {
            return Integer.compare(this.stars, o.stars);
        } else {
            return this.word.compareTo(o.word);
        }
    }
    
    @Override
    public String toString() {
        return this.word + "\t" + this.stars;
    }
    
}
