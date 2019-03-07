import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class
find_sw {

    public static class MapperClass2 extends Mapper<Object, Text, Triple_SW, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String [] split= value.toString().split("\t");
            String []ps=split[0].split("/");
            System.out.println("start map sw"+ps[0]+" "+ps[1]+" "+ps[2]);
            Triple_SW triple=new Triple_SW(ps[0],ps[1],ps[2]);
            Triple_SW triple1=new Triple_SW("*", ps[1],ps[2]);
            context.write(triple, new Text(split[1]));
            context.write(triple1, new Text(split[1]));
        }
    }


    public static class SW_Partition extends Partitioner<Triple_SW,Text> {
        // ensure that keys with same key are directed to the same reducer
        @Override

        public int getPartition(Triple_SW key, Text v, int numPartitions) {
            return  Math.abs(key.s.hashCode()+key.w.hashCode())%numPartitions;
        }
    }





    public static class CombinerClass2 extends Reducer<Triple_SW,Text,Triple_SW,Text> {
        private  long sum=0;
        private String last_s="";
        private String last_w="";
        private double N=0;
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            System.out.println("got setup here"+ conf.get("N1"));

            try {
                N= Double.valueOf(conf.get("N1"));
                System.out.println("got counter "+N);

            }
            catch (NumberFormatException ex)
            {
                ex.printStackTrace();
            }
        }

        public void reduce(Triple_SW key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values) {
                String []split=value.toString().split("//");

                if (!(this.last_w.equals(key.w) && this.last_s.equals(key.s))) {
                    this.sum = 0;
                    this.last_s = key.s;
                    this.last_w = key.w;
                }
                if(key.p.equals("*")) {
                    this.sum += Long.valueOf(split[0]);
                }
                else try {
                    context.write(new Triple_SW("*", key.s, key.w), new Text(value+"//"+String.valueOf(this.sum)));
                    context.write(new Triple_SW(key.p, key.s, key.w), value);
                    System.out.println("combiner done");
                    //
                } catch (NumberFormatException ex) {
                    System.out.println("combiner error");
                    ex.printStackTrace();
                }
            }
        }
    }




    public static class ReducerClass2 extends Reducer<Triple_SW,Text,Text,Text> {
        private  long sum=0;
        private String last_s="";
        private String last_w="";
        private double N=0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            System.out.println("got setup here"+ conf.get("N1"));

            try {
                N= Double.valueOf(conf.get("N1"));
                System.out.println("got counter "+N);

            }
            catch (NumberFormatException ex)
            {
                System.out.println("counter error");
                ex.printStackTrace();
            }
        }

        public void reduce(Triple_SW key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            System.out.println("in reduce");
            for (Text value : values) {
                System.out.println("valus in comb = "+value.toString());
                String []split=value.toString().split("//");
                if (!(this.last_w.equals(key.w) && this.last_s.equals(key.s))) {
                    this.sum = 0;
                    this.last_s = key.s;
                    this.last_w = key.w;
                }
                if(key.p.equals("*")) {
                    this.sum += Long.valueOf(split[0]);
                }
                else {
//                    System.out.println("split[0]"+split[0]);
                   // System.out.println("split[1]"+split[1]);
                    try {
                        double psw=Double.valueOf(split[0]);
                        double ps=Double.valueOf(split[1]);
                        double sw=this.sum;
                        double MI=(psw*this.N)/(ps*sw);
                        MI=Math.max(Math.log(MI+1.0)/Math.log(2.0), 0);
                        context.write(new Text(key.p+"/"+key.s+"/"+key.w), new Text(String.valueOf(MI)));
                        System.out.println("mi done");
                        //

                    }
                    catch (NumberFormatException ex)
                    {System.out.println("mi error");
                        ex.printStackTrace();
                    }
                }
            }
        }
    }
}
