import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Prob {


    public static class MapperClass extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println("started mapped prob");
            String[] val = value.toString().split("\t");
            String[]p1p2=val[0].split("/");
            context.write(new Text(p1p2[0]+"\t"+p1p2[1]), new Text(val[1]));
            System.out.println("ended mapped prob");
        }
    }



    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double prob=1;
            for(Text val:values)
            {
                prob*=Double.valueOf(val.toString());
            }
            System.out.println("combiner "+key+" ="+prob);
            context.write(key,new Text(String.valueOf(prob)));
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double prob=1;
            for(Text val:values)
            {
                prob*=Double.valueOf(val.toString());
            }
            System.out.println("reducer "+key+" ="+prob);
            context.write(key,new Text(String.valueOf(Math.sqrt(prob))));
        }
    }



}
