import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class find_ps {


    public static class MapperClass1 extends Mapper<Object, Text, Triple_PS, LongWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] split= value.toString().split("\t");
            String []ps=split[0].split("/");
            String p=ps[0];
            String s=ps[1];
            String w=ps[2];
            Triple_PS triple=new Triple_PS(p,s,w);
            Triple_PS triple1=new Triple_PS(p, s,"*");
            context.write(triple, new LongWritable(Long.valueOf(split[1])));
            context.write(triple1, new LongWritable(Long.valueOf(split[1])));
        }
    }


    public static class PS_Partition extends Partitioner<Triple_PS,LongWritable> {
        // ensure that keys with same key are directed to the same reducer
        @Override
        public int getPartition(Triple_PS key, LongWritable v, int numPartitions)
        {
            return  Math.abs(key.p.hashCode()+key.s.hashCode())%numPartitions;
        }
    }




    public static class CombinerClass extends Reducer<Triple_PS,LongWritable,Triple_PS,LongWritable> {
        private  long sum=0;
        private String last_s="";
        private String last_p="";

        @Override
        public void reduce(Triple_PS key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            for (LongWritable value : values) {
                System.out.println("key="+key.toString());
                System.out.println("value="+value.get());
                if (!(this.last_p.equals(key.p) && this.last_s.equals(key.s))) {
                    this.sum = 0;
                    this.last_s = key.s;
                    this.last_p = key.p;
                }
                if(key.w.equals("*")) {
                    this.sum += value.get();
                }
                else {
                    context.write(new Triple_PS(key.p,key.s,"*"),new LongWritable(this.sum));
                    context.write(new Triple_PS(key.p,key.s,key.w),new LongWritable(value.get()));
                }
            }
        }
    }







        public static class ReducerClass1 extends Reducer<Triple_PS,LongWritable,Text,Text> {
        private  long sum=0;
        private String last_s="";
        private String last_p="";

        @Override
        public void reduce(Triple_PS key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum2=0;
            for (LongWritable value : values) {
                System.out.println("key="+key.toString());
                System.out.println("value="+value.get());
                if (!(this.last_p.equals(key.p) && this.last_s.equals(key.s))) {
                    this.sum = 0;
                    this.last_s = key.s;
                    this.last_p = key.p;
                }
                if(key.w.equals("*")) {
                    this.sum += value.get();
                }
                else {
                    context.write(new Text(key.p+"/"+key.s+"/"+key.w), new Text(String.valueOf(value.get())+"//"+String.valueOf(this.sum)));
                }
            }
        }
    }

}
