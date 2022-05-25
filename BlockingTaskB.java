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
import org.apache.hadoop.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class BlockingTaskB extends Configured implements Tool {

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
         * @return blockid ,combo of firstname and second name
         */

/*

        StringBuilder sb = new StringBuilder();
for (int i = 0; i < length; i++) {
            sb.append(' ');
        }

return sb.substring(inputString.length()) + inputString;

*/


        public String determineBlockIdForName(String name) {

            String surname="";
            String finalName="";
            surname = name.substring(name.lastIndexOf(" ")+1);


            if(surname.length()==0)
            {
                //when name is just one word like Malik so result would be MMal

                //and name length is less than 3,apply same logic

                if(name.length()<3) {

                    //the surname should be atleast 3 char ,so filling missing
                    //words with " " ;
                    //if name is ra , its surname will be "ra "
                    //if name is A,its surname will be "r  "
                    int lengthToFill = 3;
                    String result
                            = StringUtils.rightPad(name, lengthToFill);

                    finalName = name.substring(0, 1) + result.substring(0, 3);


                }
                else
                {

                    finalName = name.substring(0, 1) + name.substring(0, 3);
                }
            }
            else
            {
                //the surname should be atleast 3 char ,so filling missing
                //words with " " ;
                //if name is mike p , its surname will be "p  ";
                //if name is coldplay si,its surname will be "si "

                if(surname.length()<3)
                {
                    int lengthToFill = 3;

                    //to add space in the surname when it is less than 3
                    String result
                            = StringUtils.rightPad(surname, lengthToFill);

                    finalName = name.substring(0, 1) + result.substring(0, 3);
                }
                else
                {
                    finalName = name.substring(0, 1) + surname.substring(0, 3);
                }
            }
            /*if(name.equals("Edward A. Fox Harry Wu")){
             */

            return finalName;

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String data[] = value.toString().split(",", -1);

            //ignore having more than 2 , means any row having 3 column
            //ignore any column which has one name null if oldname is null or new name is null
            //data.length>2 ||
            //&gt	,Rémi Lecerf,	Rémi Lecerf
            //ignore first line of csv file -comparing with oldname and newname word  NEW_NAME
            if (data.length > 2 || data[0].length() == 0 || data[1].length() == 0  || data[0].equals("# OLD_NAME")|| data[0].equals("#  NEW_NAME")) {
                return;

            }
            else
            {
                for (String n: data)
                {
                    if (n.length()<1) continue;
                    String blockidNewName=determineBlockIdForName(n.trim());
                    name.set(n);
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
         * @return blockid ,combo of firstname and second name
         */

        public String determineBlockIdForName(String name) {

            String surname="";
            String finalName="";
            surname = name.substring(name.lastIndexOf(" ")+1);

            if(surname.length()==0)
            {
                //when name is just one word like Malik so result would be MMal

                //and name length is less than 3,apply same logic

                if(name.length()<3) {

                    //the surname should be atleast 3 char ,so filling missing
                    //words with " " ;
                    //if name is ra , its surname will be "ra "
                    //if name is A,its surname will be "r  "
                    int lengthToFill = 3;
                    String result
                            = StringUtils.rightPad(name, lengthToFill);

                    finalName = name.substring(0, 1) + result.substring(0, 3);

                }
                else
                {

                    finalName = name.substring(0, 1) + name.substring(0, 3);
                }
            }
            else
            {
                //the surname should be atleast 3 char ,so filling missing
                //words with " " ;
                //if name is mike p , its surname will be "p  ";
                //if name is coldplay si,its surname will be "si "

                if(surname.length()<3)
                {
                    int lengthToFill = 3;
                    String result
                            = StringUtils.rightPad(surname, lengthToFill);

                    finalName = name.substring(0, 1) + result.substring(0, 3);
                }
                else
                {
                    finalName = name.substring(0, 1) + surname.substring(0, 3);
                }
            }

            return finalName;

        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            Text nameList = new Text();
            String toMatch="Renée J. Miller";
            String blockidNewName=determineBlockIdForName(toMatch);
            Set nameSet = new HashSet();

            if(key.toString().equals(blockidNewName)){
                for (Text val : values) {


                    String name=val.toString();
                    //putting names in set
                    nameSet.add(name);

                    ;
                }
                nameList.set(nameSet.toString());
                context.write(key, nameList);
                nameSet.removeAll(nameSet);
            }


        }

    }



    @Override
    public int run(String[] args) throws Exception
    {

        //will read hadoop configuration
        Configuration conf = getConf();

        //configure job
        //this job responsilbe for running program
        Job job = Job.getInstance(conf, "surnameFirst strategy");
        //main class
        job.setJarByClass(BlockingTaskB.class);
        job.setMapperClass(BlockingTaskB.Map.class);
        //Text.class-writer class
        //driver needs to know
        //by default it could be 3 reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(BlockingTaskB.Reduce.class);

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
        ToolRunner.run(new Configuration(),new BlockingTaskB(), args);
    }

}


