import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;



public class Pair<K,V>  {
    public  K key;
    public  V value;

    Pair(K first, V second)
    {
        this.key=first;
        this.value=second;
    }

}
