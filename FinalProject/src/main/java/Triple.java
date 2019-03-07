import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public abstract class Triple implements WritableComparable<Triple> {
    public String p;
    public String s;
    public String w;
    Triple(String p,String s, String w)
    {
        this.p=p;
        this.s=s;
        this.w=w;
    }

    Triple(){

    }

    @Override
    public String toString() {
        return p +"/"+ s +"/"+ w;
    }

   abstract public int compareTo(Triple o);

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(p);
        dataOutput.writeUTF(s);
        dataOutput.writeUTF(w);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.p=dataInput.readUTF();
        this.s=dataInput.readUTF();
        this.w=dataInput.readUTF();
    }
}