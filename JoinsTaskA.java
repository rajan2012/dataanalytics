package Assign05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class JoinsTaskA extends Configured implements Tool {



    public static class Map extends Mapper<LongWritable, Text, Text , Text> {


        final static IntWritable one = new IntWritable(1);

        Text id_key = new Text();

        Text content = new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            //System.out.println("value for mapper  "+ value);

            java.util.Map<String, String> map1 = new HashMap<String, String>();

            java.util.Map<String, String> map2 = new HashMap<String, String>();

            String l = value.toString();


            map1= MRDPUtils.transformXmlToMap(l);

            //System.out.println("map1  "+ map1);

            String a=map1.get("OwnerUserId");
            String b=map1.get("DisplayName");
            String c=map1.get("Id");


            if((map1.get("OwnerUserId")!=null && map1.get("DisplayName")==null && !map1.get("OwnerUserId").equals("-1"))){
                //System.out.println("inside owner");
                id_key.set(String.valueOf(map1.get("OwnerUserId")));
                content.set(value);
                //System.out.println("value inside not dispalyname  "+ value);
                //System.out.println("id_key inside not dispalyname  "+ id_key);
                context.write(id_key, value);
            }


           // }

            //sometimes OwnerUserId is also also null in postxml  so i have added DisplayName condition
            //since its not null in userxml
             else if(map1.get("DisplayName")!=null && map1.get("Id")!=null && map1.get("OwnerUserId")==null && !map1.get("Id").equals("-1") )
            {
                //System.out.println("inside else if");
                id_key.set(String.valueOf(map1.get("Id")));
                content.set(value);
                //System.out.println("value inside dispalyname  "+ value);
                //System.out.println("id_key inside dispalyname  "+ id_key);
                context.write(id_key, value);
            }
             else
            {
                return;
            }



            }

        }



    /**
     * It reduces a set of intermediate values that share a key to a smaller set of values
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */


    public static class Reduce extends Reducer<Text ,Text , Text , Text >{


        public double avg(ArrayList<Double> a1) {
            int sum = 0;
            double avg = 0.0;
            int length = a1.size();
            for (Double i : a1) {

                sum += i;
            }
            avg = (double)sum / length;
            return avg;
        }


        /**
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         *
         * whenever problem with overidding method ,check if dattype you are passing is mismatching
         */
        @Override
        public void reduce( Text key , Iterable <Text> values, Context context) throws IOException ,
                InterruptedException{

           // System.out.println("inside reduce method");

            java.util.Map<String, String> map2 = new HashMap<String, String>();
            int sum = 0;
            double scoreAvg=0.0;
            double scoreTot=0.0;
            double UpVotesTot=0.0;

            String  name="";

            String ownerval="";
            String id="";

            Text result = new Text();

            ArrayList<Double> OwnerIdList = new ArrayList<Double>();
            //System.out.println("key  is "+ key);

            for(Text val : values) {

                //System.out.println("value  is "+ val);
                String l = val.toString();
                map2 = MRDPUtils.transformXmlToMap(l);
               /* if(map2.get("DisplayName")!=null && map2.get("Id").equals("133150" )) {
                    System.out.println("key  is "+ key);
                    System.out.println("value  is " + val);
                }*/


              if((map2.get("OwnerUserId")!=null && map2.get("DisplayName")==null  )) {
                    if (Integer.valueOf(map2.get("OwnerUserId")) >= 20 && Integer.valueOf(map2.get("OwnerUserId")) <= 40) {
                        //System.out.println("inside OwnerUserId   ");
                        //System.out.println("key  is "+ key);
                        //System.out.println("value  is " + val);
                        ownerval = map2.get("OwnerUserId");
                        String Score = map2.get("Score");
                        //System.out.println("score are "+Score);
                        scoreTot += Double.valueOf(Score);
                        OwnerIdList.add(Double.valueOf(Score));
                        //for number of post
                        sum += 1;
                    }
                    else
                    {
                        //System.out.println("do nothing");
                        return;
                    }
                }
                else if(map2.get("DisplayName")!=null && map2.get("Id")!=null && map2.get("OwnerUserId")==null   ) {
                    if (Integer.valueOf(map2.get("Id")) >= 20 && Integer.valueOf(map2.get("Id")) <= 40) {
                        //System.out.println("key  is "+ key);
                        //System.out.println("value  is " + val);
                        id = map2.get("Id");
                        name = map2.get("DisplayName");
                        String UpVotes = map2.get("UpVotes");
                        //System.out.println("UpVotes are "+UpVotes);
                        UpVotesTot += Double.valueOf(UpVotes);
                        //System.out.println("name and  view are "+name+view+viewTot);
                    }
                    else
                    {
                        return;
                    }
                }
                else
               {
                   return;
               }


            }



            //when id is available in postxml
            //join condition
            if(ownerval!=null && id!=null && ownerval.equals(id) ) {
                if (Integer.valueOf(ownerval) >= 20 && Integer.valueOf(ownerval) <= 40) {
                    //System.out.println("key  is "+ key);
                    //System.out.println("value  is " + val);
                    scoreAvg = avg(OwnerIdList);
                    //System.out.println("OwnerIdList  are "+OwnerIdList);
                    //System.out.println("scoretotal  are "+scoreTot);
                    result.set("average score: " + scoreAvg +" total score: " + scoreTot + " no of post: " + sum + " UpVotes: " + UpVotesTot +" id: " + ownerval);
                    //System.out.println("result  is " + result);
                    key.set(name);
                    context.write(key, result);
                }
            }
            //empty OwnerIdList after each key iteration
            OwnerIdList.removeAll(OwnerIdList);
        }
    }



    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = Job.getInstance(conf, "ReduceJoin");
        job.setJarByClass(JoinsTaskA.class);
        job.setMapperClass(JoinsTaskA.Map.class);
        job.setReducerClass(JoinsTaskA.Reduce.class);
        //mapper
        job.setMapOutputKeyClass(Text.class);
        //whenever have issue of no data written check what is output datatype for
        //mapper and reducer
        job.setMapOutputValueClass(Text.class);
        //reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path output=new Path(args[2]);
        Path input=new Path(args[0]);
        Path input2=new Path(args[1]);


        //delete output file before starting job to write into file
        output.getFileSystem(conf).delete(output,true);


        //set path for input and output file
        //FileInputFormat.addInputPath(job, input);
        //FileInputFormat.addInputPath(job, input2);
        //FileOutputFormat.setOutputPath(job, output);

        //multiple file input
        MultipleInputs.addInputPath(job, input, TextInputFormat.class, JoinsTaskA.Map.class);
        MultipleInputs.addInputPath(job,input2, TextInputFormat.class, JoinsTaskA.Map.class);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new Configuration(),new JoinsTaskA(), args);
    }

}



