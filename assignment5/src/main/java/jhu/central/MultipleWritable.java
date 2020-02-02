/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jhu.central;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Created by Guan Yue Wang (gwang39)
 * 2018/10/29
 */
public class MultipleWritable implements Writable {

int inDegree = 0;
int outDegree = 0;

public MultipleWritable() {


}
    
public MultipleWritable(int inD, int outD) {
    inDegree = inD;
    outDegree = outD;
}



public int getInDegree() {
    return inDegree;
}

public void setInDegree(int inD) {
    this.inDegree=inD;
}

public int getOutDegree() {
    return outDegree;
}

public void setOutDegree(int outD) {
    this.outDegree=outD;
}


@Override
public String toString() {
    return inDegree + "\t" + outDegree;
}

public void readFields(DataInput in) throws IOException {
    inDegree = in.readInt();
    outDegree = in.readInt();
}

public void write(DataOutput out) throws IOException {
    out.writeInt(inDegree);
    out.writeInt(outDegree);

}

public void merge(MultipleWritable other)
{
    this.inDegree += other.inDegree;
    this.outDegree += other.outDegree;
}

    
}
