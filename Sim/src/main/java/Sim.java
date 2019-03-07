import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Scanner;

public class Sim {
    public static class MapperClass extends Mapper<Object, Text, Triple, Pair> {
        //HashMap<String,List<Pair<String,String>>> hashMap;
        Multimap<String, Pair> hashMap;
        @Override
        public  void setup(Context context)throws IOException   {
            //this.hashMap=new HashMap<String, List<Pair<String,String>>>();//
           this.hashMap=HashMultimap.create();
            System.out.println("in setup-mappeer");
            Configuration conf = context.getConfiguration();
            String filename1 = conf.get("File_Name");
            try {
                URL url = new URL(filename1);
                Scanner reader = new Scanner(url.openStream());
                // read from your scanner
                String line;

                while (reader.hasNext()) {
                    line=reader.nextLine();
                    if(line==null||line.equals(""))
                    {
                        continue;
                    }
//                    System.out.println(line);
                    String[] p1p2 = line.split("\t");
                    // System.out.println("line length "+p1p2.length);
                    if (!hashMap.containsKey(p1p2)) {
                        hashMap.put(p1p2[0], new Pair(p1p2[0], p1p2[1]));
                        hashMap.put(p1p2[1], new Pair(p1p2[0], p1p2[1]));
                    }
                }
                reader.close();
            }
            catch(IOException ex) {
                ex.printStackTrace();
            }
            catch(Exception ex) {
                ex.printStackTrace(); // for now, simply output it.
                try {
                    throw ex;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
System.out.println("setup map done");
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] split = value.toString().split("\t");
            String [] path = split[0].split("/");
            String P=path[0];
            String S=path[1];
            String W=path[2];
            try {
             System.out.println(path[0]);
             if(hashMap.containsKey(path[0])) {
                 for (Pair t:hashMap.get(path[0]))
                 {
                     context.write(new Triple(t.first+">"+t.second,S,W),new Pair(P,split[1]));
                 }
             }
         }
            catch (Exception ex)
            {
                //System.out.println("path = "+path[0]+" "+path[1]);

                ex.printStackTrace();
            }

         }

    }
    public static class SimPartitioner extends Partitioner<Triple,Pair>
    {
        // ensure that keys with same key are directed to the same reducer
        @Override
        public int getPartition(Triple key, Pair v, int numPartitions)
        {
            return  Math.abs(key.p.hashCode()+key.s.hashCode())%numPartitions;
        }
    }


    public static class ReducerClass extends Reducer<Triple,Pair,Text,Text> {

        HashMap<String, Double[]> hashMap;
        @Override
        public void setup(Context context) throws IOException {
            this.hashMap = new HashMap<String, Double[]>();
            System.out.println("in setup reducer");
            Configuration conf = context.getConfiguration();
            String filename1 = conf.get("File_Name");

            try {
                URL url = new URL(filename1);
                Scanner reader = new Scanner(url.openStream());
                // read from your scanner
                String line;
                while (reader.hasNext()) {
                    line=reader.nextLine();
                    if(line==null||line.equals(""))
                    {
                        continue;
                    }
                    String[] p1p2 = line.split("\t");
                    String key = p1p2[0] + "/" + p1p2[1];
                    // System.out.println(key);
                    //   System.out.println("keylen="+p1p2.length+"key="+key+"........");
                    if (!hashMap.containsKey(key)) {
                        Double[] tmp= new Double[4];
                        for (int j=0;j<tmp.length;j++)
                        {
                            tmp[j]= Double.valueOf(0);
                        }
                        hashMap.put(key, tmp);
                    }
                }
                reader.close();
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
            catch(Exception ex) {
                // there was some connection problem, or the file did not exist on the server,
                // there was soinme connection problem, or the file did not exist on the server,
                // or your URL was not in the right format.
                // think about what to do now, and put it here.
                ex.printStackTrace(); // for now, simply output it.
                try {
                    throw ex;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String key : hashMap.keySet())
            {
               Double[] a= hashMap.get(key);
               double y=0,x=0;
               if(a[0]!=0)
               {x=a[1]/a[0];}
               if(a[2]!=0)
               {
                   y=a[3]/a[2];
               }
//               if(x!=0)
               {
                   System.out.println("key"+key+"a[0]"+a[0]+"a[1]"+a[1]);
                   context.write(new Text(key + "/X"), new Text(String.valueOf(x)));
               }
  //             if(y!=0)
               {
                   System.out.println("key="+key+"a[2]"+a[2]+"a[3]"+a[3]);
                   context.write(new Text(key + "/Y"), new Text(String.valueOf(y)));
               }
            }
        }

        public void reduce(Triple key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            //System.out.println("reduce-start");
            String[] paths = key.p.split(">");
            int b=0;
            Double[] a =hashMap.get(paths[0]+"/"+paths[1]);
            if(a==null)
            {
                return;
            }
            if(key.s.equals("Y"))
            {b=2;}
            boolean f=false;
            double mi=0;
            double res=0;
//            System.out.println("reduce-1");
            for (Pair p : values)
            {
                mi+=Double.valueOf(p.second);
                if(f)
                {
                    res=mi;
                }
                else
                {
                    f=true;
                }
            }
            if(res!=0)
            {
                System.out.println("res="+res+"mi="+mi);
                System.out.println("a["+b+"]="+a[b]+"a["+b+1+"]"+a[b+1]);
            }
    //        System.out.println("reduce-2, "+a.length+",b="+b);
            a[b]+=Double.valueOf(mi);
            a[b+1]+=Double.valueOf(res);
      //      System.out.println("reduce-end");
        }
    }
    public static void main(String[] args) throws Exception {
        for(int i=0;i<args.length;i++)
        {
            System.out.println(args[i]);
        }
        Configuration conf;
        conf = new Configuration();
        conf.set("File_Name",args[3]);

        //String c=conf.get("File_Name");
        // System.out.println("file name main = "+c);
        Job job = Job.getInstance(conf);
        job.setJobName("Sim");
        job.setJarByClass(Sim.class);
        job.setMapperClass(Sim.MapperClass.class);
        job.setReducerClass(Sim.ReducerClass.class);
        //job.setCombinerClass(Sim.ReducerClass.class);
        job.setMapOutputKeyClass(Triple.class);
        job.setMapOutputValueClass(Pair.class);
        job.setOutputKeyClass(Text.class);
        job.setPartitionerClass(SimPartitioner.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("tmp"));
        job.waitForCompletion(true);
        System.out.println("ended1");

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1);

        job1.setJobName("sim_sum");
        job1.setJarByClass(sim_sum.class);
        job1.setMapperClass(sim_sum.MapperClass.class);
        job1.setReducerClass(sim_sum.ReducerClass.class);
        job1.setCombinerClass(sim_sum.ReducerClass.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1,new Path("tmp"));
        FileOutputFormat.setOutputPath(job1,new Path("prob"));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
System.out.println("done job1");
        job2.setJobName("sim_Prob");
        job2.setJarByClass(Prob.class);
        job2.setMapperClass(Prob.MapperClass.class);
        job2.setReducerClass(Prob.ReducerClass.class);
        job2.setCombinerClass(Prob.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setNumReduceTasks(1);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2,new Path("prob"));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.waitForCompletion(true);
    }
}