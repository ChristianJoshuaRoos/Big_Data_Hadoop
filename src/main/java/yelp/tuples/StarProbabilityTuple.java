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
public class StarProbabilityTuple implements WritableComparable<StarProbabilityTuple> {
    public Integer stars;
    public Double probability;
    
    public StarProbabilityTuple() {}
    public StarProbabilityTuple(Integer stars, Double probability) {
        this.stars = stars;
        this.probability = probability;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.stars);
        out.writeDouble(this.probability);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stars = in.readInt();
        this.probability = in.readDouble();
    }

    @Override
    public int compareTo(StarProbabilityTuple other) {
        if (this.stars.equals(other.stars)) {
            return this.probability.compareTo(other.probability);
        }
        return this.stars.compareTo(other.stars);
    }
    
    public String toString() {
        return this.stars + ":" + this.probability;
    }
}
