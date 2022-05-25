package Assign05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//mapper and reducer are class
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class nGramsTask extends Configured implements Tool {

    /**
     * defines the Map job. Maps input key-value pairs to a set of intermediate key-value pairs
     * Mapper -map each word of file to 1
     *
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     **/

    public static class Map extends Mapper<LongWritable, Text , Text , IntWritable > {


        final static IntWritable one = new IntWritable(1);
        Text word = new Text();
        int toeknsLength;
        int nMin = 2;
        int nMax = 4;

        /**
         * @param linesString1
         * @return ngram string for each line
         */
        public ArrayList<String> nGramCal(ArrayList<String> linesString1,int nmin,int nmax) {

            String ngram = "";
            int pos = 0;
            ArrayList<String> linesStringfinal = new ArrayList<String>();


            for (int k = 0; k < linesString1.size(); k++) {
                pos = k;
                ngram = "";
                //System.out.println("k " + k);
                int kmin = nmin;
                for (int j = nmin; j <= nmax; j++) {


                    while (pos < k + j && pos < linesString1.size()) {
                        ngram += linesString1.get(pos) + " ";
                        int length1 = ngram.trim().split("\\s+").length;

                        if (length1 == kmin) {
                            kmin++;
                            //System.out.println("inside  ngram " + ngram);
                            linesStringfinal.add(ngram.trim());
                        }

                        pos += 1;
                    }
                }

            }
            return linesStringfinal;
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //convert doc into bunch of string
            //remove all punctuation and numbers
            //if i use replaceAll("[^\\w\\s]"," ") to remove special character , then it gives different numbers
            String l = value.toString().replaceAll("\\p{Punct}", " ").trim().toLowerCase();
            //if i remove digit it gives me more result
            //.replaceAll("\\d"," ")

            int nmin = 2;
            int nmax = 4;

            //divide each line string into word
            StringTokenizer tokenizer = new StringTokenizer(l);
            toeknsLength = tokenizer.countTokens();

            String[] linesString =new String[toeknsLength];
            ArrayList<String> linesString1 = new ArrayList<String>();

            ArrayList<String> ngramList = new ArrayList<String>();


            while (tokenizer.hasMoreTokens()) {

                //store into string arraylist
                linesString1.add(tokenizer.nextToken());

            }

            //System.out.println("string linesString1 " + linesString1);

            //call method to give ngrams
            ngramList=nGramCal(linesString1,nmin,nmax);

            //System.out.println("string ngramList " + ngramList);

            //iterate through ngramList and add to mapper buffer
            for (String temp : ngramList) {
                word.set(temp);
                context.write(word, one);
            }

            //empty linesString1
            linesString1.removeAll(linesString1);



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
            int frequency=1000;

            for(IntWritable val : values)
            {
                /*if(String.valueOf(key).equals(" "))
                {
                    System.out.println("val is " + val);
                }*/

                //sum for each word
                sum +=val.get();
            }
            if(sum>=frequency) {
                context.write(key, new IntWritable(sum));
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        //configure job
        Job job = Job.getInstance(conf, "n gram Naive Approach");
        //main class
        job.setJarByClass(nGramsTask.class);
        job.setMapperClass(nGramsTask.Map.class);
        //Text.class-writer class
        //driver needs to know
        //by default it could be 3 reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(nGramsTask.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
        ToolRunner.run(new Configuration(),new nGramsTask(), args);
    }

}