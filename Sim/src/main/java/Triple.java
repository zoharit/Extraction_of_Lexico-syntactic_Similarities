import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



    public class Triple implements WritableComparable<Triple> {
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

        public int compareTo(Triple o) {
            int x=this.p.compareTo(o.p);
            int y=this.s.compareTo(o.s);
            int z=this.w.compareTo(o.w);


            if(x!=0)
            {
                return x;
            }
            if(y!=0)
            {
                return y;
            }
            if (z!=0)
            {
                return z;
            }
            return 0;
        }

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

