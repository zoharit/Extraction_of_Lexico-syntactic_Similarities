
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class sim_sum {

    public static class MapperClass extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println("started mapped sum");
            String[] val = value.toString().split("\t");
            context.write(new Text(val[0]), new Text(val[1]));
            System.out.println("ended mapped sum");
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum=0;
            for(Text val:values)
            {
                sum+=Double.valueOf(val.toString());
            }
            System.out.println("reducer "+key+" ="+sum);
            context.write(key,new Text(String.valueOf(sum)));
        }
    }
}
