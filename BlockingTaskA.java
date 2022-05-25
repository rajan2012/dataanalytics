package mrdp.utils.src.Assign04;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.HashSet;
import java.util.Set;

//mapper and reducer are class

public class BlockingTaskA extends Configured implements Tool {

    /**
     * defines the Map job. Maps input key-value pairs to a set of intermediate key-value pairs
     * Mapper -map each word of file to 1
     *
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     **/


    public static class Map extends Mapper<LongWritable, Text , Text , Text > {


        //The Text class defines a node that displays a text. Paragraphs are separated
        // by '\n' and the text is wrapped on paragraph boundaries.
        Text name = new Text();

        Text blockID = new Text();

        /**
         *
         * @param name
         * @return surname
         */

        public String determineBlockIdForName(String name) {

            String surname="";
            //give surname of each name
            surname = name.substring(name.lastIndexOf(" ")+1);
            return surname;

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            //split each row of csv file
            String data[] = value.toString().split(",", -1);

            //ignore having more than 2 , means any row having 3 column
            //ignore any column which has one name null
            if (data.length > 2 || data[0].length() == 0 || data[1].length() == 0  || data[0].equals("# OLD_NAME")) {
                return;

            }
            else
            {
                for (String n: data)
                {
                    if (n.length()<1) continue;
                    String blockidNewName=determineBlockIdForName(n.trim());
                    //value as name
                    name.set(n);
                    //key
                    blockID.set(blockidNewName);
                    //if(blockidNewName.equals("Miller")){
                        context.write(blockID,name);
                   // }

                }


                }
            }
        }


    /**
     * It reduces a set of intermediate values that share a key to a smaller set of values
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */

    public static class Reduce extends Reducer<Text ,Text , Text , Text > {

        /**
         *
         * @param name
         * @return surname
         */
        public String determineBlockIdForNameReducer(String name) {

            String surname="";
            surname = name.substring(name.lastIndexOf(" ")+1);
            return surname;

        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            Text nameList = new Text();
            Text matchedblockID = new Text();

            int sum = 0;
            int flag=0;
            String toMatch="Renée J. Miller";
            String blockidNewName=determineBlockIdForNameReducer(toMatch);
            Set nameSet = new HashSet();


            ArrayList<String> matchedNameBlock = new ArrayList<String>();


            /**
             *approach 1- getting surname of Renée J. Miller ,and then since its key and block identifier
             * and then get all the value of miller block and store in set datasets and pass it in context
             *
             * approach 2- iterate through key and get block id for  Renée J. Miller ,and then again
             * iterate through key and match key with block id and  store in set datasets and pass it in context
             */


            /**
             * 1st approach
          /*   */
            if(key.toString().equals(blockidNewName)){
            for (Text val : values) {

                //sum for each word

                String name=val.toString();
                String matchedBlock="";
                    nameSet.add(name);

                    ;
            }
                nameList.set(nameSet.toString());
                context.write(key, nameList);
                nameSet.removeAll(nameSet);
               }

            /**
             * 2nd approach
             * //gewt matched block id
             */
       /*     for (Text val : values) {

                //sum for each word

                String name=val.toString();
                if(name.equals(toMatch))
                {
                    matchedblockID=key;
                    break;

                }

            }

            //put all names of bblock of raenne miller
            //get names for matched id
            if(key.equals(matchedblockID)) {
                for (Text val : values) {

                    //sum for each word

                    String name = val.toString();
                    String matchedBlock = "";
                    nameSet.add(name);
                }
                nameList.set(nameSet.toString());
                context.write(key, nameList);
                nameSet.removeAll(nameSet);
            }*/



        }

    }



    @Override
    public int run(String[] args) throws Exception
    {

        //will read hadoop configuration
        Configuration conf = getConf();

        //configure job
        //this job responsilbe for running program
        Job job = Job.getInstance(conf, "surname strategy");
        //main class
        job.setJarByClass(BlockingTaskA.class);
        job.setMapperClass(BlockingTaskA.Map.class);
        //Text.class-writer class
        //driver needs to know
        //by default it could be 3 reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(BlockingTaskA.Reduce.class);

        //below 2 line could be used by mapper too if the data type for key and value
        //is same for mapper too
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path output=new Path(args[1]);
        Path input=new Path(args[0]);

        //delete output file before starting job to write into file
        output.getFileSystem(conf).delete(output,true);


        //set path for input and output file
        //FileInputFormat-file format
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception
    {
        //run in hadoop configuration
        ToolRunner.run(new Configuration(),new BlockingTaskA(), args);
    }

}
