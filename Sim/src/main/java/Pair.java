import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;



public class Pair implements WritableComparable<Pair> {
    public  String first;
    public  String  second;

    Pair(String first, String second)
    {
        this.first=first;
        this.second=second;
    }
    Pair()
    {
        this.first="";
        this.second="";
    }
    public int compareTo(Pair o)
    {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(first);
        dataOutput.writeUTF(second);
    }

    public void readFields(DataInput dataInput) throws IOException
    {
        this.first=dataInput.readUTF();
        this.second=dataInput.readUTF();

    }
}
