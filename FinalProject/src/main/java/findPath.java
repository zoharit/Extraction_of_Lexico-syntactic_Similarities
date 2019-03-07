import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


public class findPath {

    enum UpdateCount{
        CNT
    }
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        StanfordLemmatizer lematizer;

        public  void setup(Context context) {
        this.lematizer=new StanfordLemmatizer();
        }
        @Override
        public void map(LongWritable key, Text value, Context context) {
            //   System.out.println("map start");
            try {
                String regex = "^[a-zA-Z0-9\\s]+$";
                Pattern pattern = Pattern.compile(regex);

                String[] row = value.toString().split("\t");
                //String head = row[0];
                String[] nodes = row[1].split("\\s+");

                ArrayList<TreeNode<WordData>> TreeNode_lst = new ArrayList<TreeNode<WordData>>();
                // System.out.println("nodes.length=" + nodes.length);
               // boolean working = true;
                for (int i = 0; i < nodes.length; i++) {
                    String[] tmp = nodes[i].split("/");
                    //   System.out.println(i);
                    try {

                        String word = tmp[0];
                        String type = tmp[1];
                        int parentid = Integer.valueOf(tmp[3]);

                        TreeNode_lst.add(i, new TreeNode<WordData>(new WordData(word, type, parentid, i,lematizer)));
                    } catch (NumberFormatException ex) {
                        //      ex.printStackTrace();
                        //     System.out.println("bad data");
                        //   System.out.println(key);
                        //  System.out.println(value);
                        return;
                    }

                    //         System.out.println("after"+i);

                }
                // System.out.println("map start2");
                for (int i = 0; i < nodes.length; i++) {
                    TreeNode<WordData> tmp = TreeNode_lst.get(i);
                    if (tmp.data.parentid > 0) {
                        tmp.addParent(TreeNode_lst.get(tmp.data.parentid - 1));
                    }
                }

                ArrayList<Integer> nouns_indexs = new ArrayList<Integer>();
                int noun_counter = 0;
                int root_counter = 0;
                int root_index = -1;
                for (int i = 0; i < nodes.length; i++) {
                    if (TreeNode_lst.get(i).parent == null) {
                        root_counter++;
                        root_index = i;
                    }
                    if (TreeNode_lst.get(i).data.is_noun()) {
                        //  System.out.println("found noun="+TreeNode_lst.get(i).data.toString());
                        nouns_indexs.add(i);
                        noun_counter++;
                    }
                }
                try {
                    if (root_counter != 1) {
                           System.out.print("error at sentence with value"+value);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("error for line:"+value.toString());
                    return;
                }
                /* System.out.println("printing tree root index="+root_index); */
                TreeNode_lst.get(root_index).print_tree();
                //   System.out.println("map start4");
                List<Pair<Integer, Integer>> nounspairs = new ArrayList<Pair<Integer, Integer>>();
                for (int i = 0; i < nouns_indexs.size(); i++) {
                    for (int j = i + 1; j < nouns_indexs.size(); j++) {
                        if (i != j) {
                            // System.out.println("found noun pair i="+i+",j="+j);
                            nounspairs.add(new Pair<Integer, Integer>(nouns_indexs.get(i), nouns_indexs.get(j)));
                        }
                    }
                }
                //  System.out.println("map start5 working="+working);
                // System.out.println("noun counter="+noun_counter);
                // System.out.println("root_index.data"+TreeNode_lst.get(root_index).data);
                if (TreeNode_lst.get(root_index).data.is_verb() && noun_counter >= 2) {
                    for (Pair<Integer, Integer> p : nounspairs) {
                        for (int i = 0; i < nodes.length; i++) {
                            TreeNode_lst.get(i).visited = false;
                        }
                        Integer key1 = p.key;
                        Integer key2 = p.value;
                        TreeNode<WordData> goal = TreeNode_lst.get(key2);
                        TreeNode<WordData> start = TreeNode_lst.get(key1);
                        //   System.out.println("start="+start.data.toString());
                        //   System.out.println("end="+goal.data.toString());
                        String path = searchPath(start, goal, null, false, root_index);
                        if (path != null) {
                            path = "X " + path.trim() + " Y";

                            //System.out.println("path found ="+path);
                            //System.out.println("x="+start.data.toString());
                            //System.out.println("y="+goal.data.toString());
                            String triple = path + "/" + "X" + "/" + start.data.toString();
                            String triple1 = path + "/" + "Y" + "/" + goal.data.toString();
                            String[] reversed_path=path.split("\\s+");
                            StringBuilder px= new StringBuilder();
                            for(int i=reversed_path.length-1;i>=0;i--)
                            {
                                px.append(reversed_path[i]).append(" ");
                            }
                            px = new StringBuilder(px.toString().trim());
                            String triple2 = px + "/" + "X" + "/" + goal.data.toString();
                            String triple3 = px + "/" + "Y" + "/" + start.data.toString();

                            if(pattern.matcher(start.data.word).matches()&&pattern.matcher(goal.data.word).matches()&&pattern.matcher(path).matches()) {
                                context.getCounter(UpdateCount.CNT).increment(Integer.valueOf(row[2]));
                                // System.out.println("counter = "+context.getCounter(UpdateCount.CNT).getValue());
                                // context.write(new Text(row[1]), new Text(path));

                                context.write(new Text(triple), new Text(row[2]));
                                context.write(new Text(triple1), new Text(row[2]));

                                context.write(new Text(triple2), new Text(row[2]));
                                context.write(new Text(triple3), new Text(row[2]));

                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                System.out.println("exception = "+value);
                e.printStackTrace();
            }
        }

        String searchPath(TreeNode<WordData>start,TreeNode<WordData>goal,String acc,boolean head,int headid)
        {

            if(start==null||start.visited)
            {
                return  null;
            }
            start.visited=true;
            if(start.data.id==goal.data.id)
        {

            if(head)
            {
                return  acc;
            }
            else
            {
                return  null;
            }
        }
    //    System.out.print("got to here4");
        if(start.data.id==headid)
        {
      //      System.out.print("got to here5");
        head=true;
        }
        if(!head) {
        //    System.out.print("got to here6");
            if (acc == null) {
                return searchPath(start.parent, goal, "", false, headid);
            }
            else {
                return searchPath(start.parent, goal, acc+" "+start.data.word, false, headid);
            }
        }
        else
            {
            for (TreeNode<WordData> n : start.children) {
                String helper;
                if (acc == null) {
                    helper= searchPath(n, goal, "", true, headid);
                } else {
                    helper= searchPath(n, goal, acc+" " + start.data.word, true, headid);
                }
                if(helper!=null)
                {
                    return  helper;
                }
            }
        }
            return  null;
        }
    }





    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override

            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int counter=0;
            for (Text value : values)
            {
                counter+=Long.valueOf(value.toString());
            }
           // System.out.println("counter value = "+counter);
            context.write(key, new Text(String.valueOf(counter)));
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("starting");
        System.out.println("args len="+args.length);
        for (String arg : args) {
            System.out.println(arg);
        }
        Configuration conf;
        conf = new Configuration();
        Job job =Job.getInstance(conf);
        job.setJobName("find paths");
        job.setJarByClass(findPath.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        /*for(int i=0;i<12;i++)
        {
            String xx;
            if(i<10)
            {
                xx="0"+String.valueOf(i);
            }
            else
                xx=String.valueOf(i);
            System.out.println("added path:"+"s3n://dsp191/syntactic-ngram/biarcs/biarcs."+xx+"-of-99");
            FileInputFormat.addInputPath(job, new Path( "s3n://dsp191/syntactic-ngram/biarcs/biarcs."+xx+"-of-99"));
        }

*/
        FileOutputFormat.setOutputPath(job, new Path("res"));
        job.waitForCompletion(true) ;
        conf.set("N", String.valueOf(job.getCounters().findCounter(UpdateCount.CNT).getValue()));
        long c=Long.valueOf(conf.get("N"));
        System.out.println("counter in main is = "+c);
        System.out.println("ended1");

        Configuration conf1 = new Configuration();
         Job job1 =Job.getInstance(conf1);
         job1.setJobName("P_S");
         job1.setJarByClass(find_ps.class);
       // job1.setCombinerClass(find_ps.CombinerClass.class);
        job1.setMapperClass(find_ps.MapperClass1.class);
        job1.setReducerClass(find_ps.ReducerClass1.class);
        job1.setMapOutputKeyClass(Triple_PS.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setPartitionerClass(find_ps.PS_Partition.class);
        FileInputFormat.addInputPath(job1, new Path("res"));
        FileOutputFormat.setOutputPath(job1, new Path("output1"));
        job1.waitForCompletion(true) ;
        Configuration conf2 = new Configuration();
        conf2.set("N1", conf.get("N"));
        Job job2 = Job.getInstance(conf2);
        job2.setJobName("M_I calc");
        job2.setJarByClass(find_sw.class);
       // job2.setCombinerClass(find_sw.CombinerClass2.class);
        job2.setMapperClass(find_sw.MapperClass2.class);
        job2.setReducerClass(find_sw.ReducerClass2.class);
        job2.setMapOutputKeyClass(Triple_SW.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setPartitionerClass(find_sw.SW_Partition.class);
        FileInputFormat.addInputPath(job2, new Path("output1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}