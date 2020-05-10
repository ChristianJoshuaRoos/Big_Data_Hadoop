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
 *
 * @author Christian Roos
 */
public class StarCountTuple implements WritableComparable<StarCountTuple>{
    public Integer stars;
    public Integer count;
    
    public StarCountTuple() {};
    public StarCountTuple(Integer stars, Integer count) {
        this.stars = stars;
        this.count = count;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.stars);
        out.writeInt(this.count);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.stars = in.readInt();
        this.count = in.readInt();
    }
    
    @Override
    public int compareTo(StarCountTuple other) {
        if (this.stars.equals(other.stars)) {
            return this.count.compareTo(other.count);
        } else {
            return this.stars.compareTo(other.stars);
        }
    }
    
    @Override
    public String toString() {
        return this.stars + "\t" + this.count;
    }
    
}
