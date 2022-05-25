package mrdp.utils.src.Assign04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;

public class Aggregation_TaskA extends Configured implements Tool {



    public static class Map extends Mapper<LongWritable, Text, Text , IntWritable> {


        final static IntWritable one = new IntWritable(1);

        Text id_key = new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



            java.util.Map<String, String> map1 = new HashMap<String, String>();

            String l = value.toString();


            map1=MRDPUtils.transformXmlToMap(l);


            //sometimes this 2 field is not there in line so checking if not null then proceed
            //map1.get("Id")!=null &&
            //OwnerUserId should not be -1 so ignoring those records too
            if( map1.get("OwnerUserId")!=null && (!map1.get("OwnerUserId").equals("-1"))) {
                String IdVal = "";
                String OwnerUserIdVal = "";
                IdVal = map1.get("Id");
                //if id >=10 having at most 2 digits
                //if (Integer.parseInt(map1.get("OwnerUserId")) < 100) {
                //i have ignored where OwnerUserId value is -1 which doesn't make sense
                if (map1.get("OwnerUserId").length()<=2) {
                    OwnerUserIdVal = map1.get("OwnerUserId");
                    id_key.set(String.valueOf(OwnerUserIdVal));
                    //one.set(Integer.valueOf(IdVal));

                    //writitng key and value
                    //for each ownerid ,its value would be 1
                    context.write(id_key, one);
                }
            }


            }

        }



    /**
     * It reduces a set of intermediate values that share a key to a smaller set of values
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */


    public static class Reduce extends Reducer<Text ,IntWritable , Text , IntWritable >{
        @Override
        public void reduce( Text key , Iterable <IntWritable> values, Context context) throws IOException ,
                InterruptedException{
            int sum = 0;

            for(IntWritable val : values)
            {
                //System.out.println("value  is "+ val);
                //how many publication for each users
                sum +=1;
            }

            context.write(key , new IntWritable(sum));
        }
    }



    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = Job.getInstance(conf, "Post_Per_User");
        job.setJarByClass(Aggregation_TaskA.class);
        job.setMapperClass(Aggregation_TaskA.Map.class);
        job.setReducerClass(Aggregation_TaskA.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path output=new Path(args[1]);
        Path input=new Path(args[0]);


        //delete output file before starting job to write into file
        output.getFileSystem(conf).delete(output,true);


        //set path for input and output file
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new Configuration(),new Aggregation_TaskA(), args);
    }

}



