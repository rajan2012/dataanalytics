package mrdp.utils.src.Assign04;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

//import java.util.Map;

public class Aggregation_TaskB extends Configured implements Tool {



    public static class Map extends Mapper<LongWritable, Text, Text , IntWritable> {

        final static IntWritable one = new IntWritable(1);

        Text id_key = new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            java.util.Map<String, String> map1 = new HashMap<String, String>();

            String l = value.toString();

            map1=MRDPUtils.transformXmlToMap(l);

            //sometimes this 2 field is not there in line so checking if not null then proceed
            //&& map1.get("Title").length()!=0
            //map1.get("Id")!=null &&
            if( map1.get("Title")!=null && map1.get("OwnerUserId")!=null ) {
                //String IdVal = "";
                int titleLength = 0;
                String OwnerUserIdVal = "";
                //putting length of title in variable
                titleLength = Integer.valueOf(map1.get("Title").length());
                //if id >=10 having at least 2 digits
                if (map1.get("OwnerUserId").length()<=2) {
                    OwnerUserIdVal = map1.get("OwnerUserId");
                    id_key.set(String.valueOf(OwnerUserIdVal));
                    one.set(Integer.valueOf(titleLength));
                    context.write(id_key, one);
                }
            }


        }

    }



    /**
     * It reduces a set of intermediate values that share a key to a smaller set of values
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */


    public static class Reduce extends Reducer<Text ,IntWritable , Text , DoubleWritable> {

        /**
         * @param
         * @return statistical measurement function -median
         */


        public double median(ArrayList<Integer> a1) {

            //sort arraylist for finding median
            Collections.sort(a1);

            int length=a1.size();
            double median=0.0;
           if(length%2==0)
           {
               //when length is even
               median=  (double)(a1.get(length / 2) +a1.get((length / 2)-1))/2;
           }
           else

           {
               //when length is odd
               median=(double)a1.get(((length+1)/2)-1);

           }

            return median;
        }

        /**
         *
         * @param a1
         * @return statistical measurement function -mean
         */
        public double mean(ArrayList<Integer> a1) {
            int sum = 0;
            double avg = 0.0;
            int length = a1.size();
            for (Integer i : a1) {

                sum += i;
            }
            avg = (double)sum / length;
            return avg;
        }


        /**
         *
         * @param a1
         * @return statistical measurement function -max element
         */

        public double max(ArrayList<Integer> a1) {

            return Collections.max(a1);
        }

        /**
         *
         * @param a1
         * @return statistical measurement function -min element
         */


        public double min(ArrayList<Integer> a1) {

             return Collections.min(a1);
        }


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            DoubleWritable result = new DoubleWritable();
            Text resultKey = new Text();
            double avg = 0.0;
            double max=0.0;
            double min=0.0;
            double median=0.0;

            ArrayList<Integer> OwnerIdList = new ArrayList<Integer>();

            for (IntWritable val : values) {
                //putting value for each key in arraylist
                OwnerIdList.add(val.get());
            }



            avg = mean(OwnerIdList);
            max = max(OwnerIdList);
            min = min(OwnerIdList);
            median=median(OwnerIdList);


            System.out.println("key "+key);
            System.out.println("value "+OwnerIdList);
            System.out.println("avg of "+ key+  " are " +" " + avg);
            System.out.println("max of "+ key+  " are " +" " + max);
            System.out.println("min of "+ key+  " are " +" " + min);
            System.out.println("median of "+ key+  " are " +" " + median);

            result.set(avg);
            resultKey.set(key.toString()+":average");

            context.write(resultKey, result);
            result.set(max);
            resultKey.set(key.toString()+":maximum");
            context.write(resultKey, result);

            result.set(min);
            resultKey.set(key.toString()+":minimal");
            context.write(resultKey, result);

            result.set(median);
            resultKey.set(key.toString()+":median");
            context.write(resultKey, result);

            //empty arraylist after each key is processed
            OwnerIdList.removeAll(OwnerIdList);
        }
    }

        @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = Job.getInstance(conf, "statistical measurement analysis");
        job.setJarByClass(Aggregation_TaskB.class);
        job.setMapperClass(Aggregation_TaskB.Map.class);
        job.setReducerClass(Aggregation_TaskB.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

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
        ToolRunner.run(new Configuration(),new Aggregation_TaskB(), args);
    }

}




