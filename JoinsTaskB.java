package Assign05;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.*;
import java.util.*;

public class JoinsTaskB extends Configured implements Tool {


    /*//input path for userxml
     static String a="";
     static String  inputPath="";
//    public static String inputPath2="";
    public JoinsTaskB(String a) {

        inputPath=a;
        System.out.println("inputPath  is " + inputPath);


    }*/



    public  static class Map extends Mapper<LongWritable, Text, Text , NullWritable> {


            TreeSet<Integer> treeSet = new TreeSet<Integer>();
            final static IntWritable one = new IntWritable(1);

            Text id_key = new Text();

            Text content = new Text();

            java.util.Map<String, String> map3 = new HashMap<String, String>();


            //constructor for id set
            public Map() throws IOException {

                //JoinsTaskB.inPath();
                Set idSet = new HashSet();
                java.util.Map<String, String> map2 = new HashMap<String, String>();
                StringBuilder resultStringBuilder = new StringBuilder();
                String input="/Users/rajansah/Documents/msc/2ndSem/DataAnalytics/untitledfolder5/users.xml";
                //String input=inputPath;
                FileInputStream fstream = new FileInputStream(input);
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                String line;

                while ((line = br.readLine()) != null) {
                    resultStringBuilder.append(line).append("\n");
                    //System.out.println("line  is " + line);
                    map2 = MRDPUtils.transformXmlToMap(line);




                    //append id in set
                    if(map2.get("Id")!=null && !map2.get("Id").equals("-1") ){
                        treeSet.add(Integer.valueOf(map2.get("Id")));
                    }


                }
                //System.out.println("treeSet  is " + treeSet);


            }

            /**
             *
             * @param key
             * @param value
             * @param context
             * @throws IOException
             * @throws InterruptedException
             */

            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


                //System.out.println("value for mapper  "+ value);
                java.util.Map<String, String> map1 = new HashMap<String, String>();
                TreeSet<Integer> treeSet2 = new TreeSet<Integer>();


                 String l = value.toString();

                 map1= MRDPUtils.transformXmlToMap(l);



                //only keeping value from 110 to 120
                for (Integer s: treeSet)
                {
                    // filter languages that start with `C`
                    if (s>=110 && s<=120) {
                        treeSet2.add(s);
                    }
                }
                //System.out.println("treeSet2  is " + treeSet2);

                if((map1.get("OwnerUserId")!=null &&  !map1.get("OwnerUserId").equals("-1"))) {
                    //System.out.println("inside OwnerUserId "+map1.get("OwnerUserId"));

                    //check if owneruserid in userbox
                    if (treeSet2.contains(Integer.valueOf(map1.get("OwnerUserId")))) {
                        //System.out.println("map1  "+ map1);
                        id_key.set(String.valueOf(map1.get("Id")));
                        //System.out.println("id_key  "+ id_key);
                        context.write(id_key, NullWritable.get());
                    }
                }
            }

        }




        @Override
        public int run(String[] args) throws Exception
        {
            Configuration conf = getConf();


            Job job = Job.getInstance(conf, "ReduceJoin");
            job.setJarByClass(Assign05.JoinsTaskB.class);
            job.setMapperClass(Assign05.JoinsTaskB.Map.class);
            //job.setReducerClass(Assign05.JoinsTaskB.Reduce.class);
            //mapper
            job.setMapOutputKeyClass(Text.class);
            //whenever have issue of no data written check what is output datatype for
            //mapper and reducer
            job.setMapOutputValueClass(NullWritable.class);
            //reducer
            //job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(Text.class);

            Path output=new Path(args[1]);
            Path input=new Path(args[0]);
            //Path input2=new Path(args[1]);


            //delete output file before starting job to write into file
            output.getFileSystem(conf).delete(output,true);


            //set path for input and output file
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            return job.waitForCompletion(true)?0:1;
        }

        public static void main(String[] args) throws Exception
        {
            ToolRunner.run(new Configuration(),new Assign05.JoinsTaskB(), args);
        }

    }










