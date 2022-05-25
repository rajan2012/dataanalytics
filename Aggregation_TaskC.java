

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
import java.util.HashMap;
//import java.util.Map;

public class Aggregation_TaskC extends Configured implements Tool {



    public static class Map extends Mapper<LongWritable, Text, Text , Text> {

        final static IntWritable one = new IntWritable(1);

        private final Text word = new Text("1");

        Text id_key = new Text();

        Text score_view=new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            java.util.Map<String, String> map1 = new HashMap<String, String>();

            String l = value.toString();

            map1=MRDPUtils.transformXmlToMap(l);

            //sometimes this 2 field is not there in line so checking if not null then proceed
            //map1.get("Id")!=null && | && map1.get("OwnerUserId")!=null
            if( map1.get("Score")!=null  && map1.get("ViewCount")!=null) {
                String Score = map1.get("Score");
                String ViewCount = map1.get("ViewCount");

                String OwnerUserIdVal = "";
                //if id >=10 having at least 2 digits
                //if (Integer.parseInt(map1.get("Id")) > 9) {
                    OwnerUserIdVal = map1.get("OwnerUserId");
                    id_key.set(String.valueOf(OwnerUserIdVal));
                    //pair of score and viewcount
                    score_view.set(Score+"&"+ViewCount);
                    context.write(word, score_view);
                }
            //}


        }

    }



    /**
     * It reduces a set of intermediate values that share a key to a smaller set of values
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */


    public static class Reduce extends Reducer<Text ,Text , Text , DoubleWritable> {

        /**
         * @param
         * @return correlation
         */

        public double scoreViewCor(ArrayList<Double> scoreList,ArrayList<Double> viewList,double scoreAvg,double viewAvg) {

            double corr=0.0;
            double varScore=0.0;
            //double varScore1=0.0;
            double varView=0.0;
            double covScView=0.0;

           for(int i=0;i<viewList.size();i++)
           {
               covScView+=(scoreList.get(i)-scoreAvg)*(viewList.get(i)-viewAvg);
               varScore+=Math.pow((scoreList.get(i)-scoreAvg),2);
               varView+=Math.pow((viewList.get(i)-viewAvg),2);
           }
            System.out.println("covScView  "+covScView );
            System.out.println("varScore  "+varScore );
            System.out.println("varView  "+varView );

            corr+=covScView/Math.sqrt(varScore*varView);
            return corr;
        }



        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            DoubleWritable result = new DoubleWritable();
            Text resultKey = new Text();

            ArrayList<Double> ScoreList = new ArrayList<Double>();

            ArrayList<Double> ViewList = new ArrayList<Double>();

            double score=0.0;
            double viewCount=0.0;
            double scoreTotal=0.0;
            double viewCountTotal=0.0;
            for (Text val : values) {
                String score_view=val.toString();
                int idx=score_view.indexOf('&');
                if (idx!=-1)
                {

                    score = Double.valueOf(score_view.substring(0, idx));
                    //adding score in arraylist
                    ScoreList.add(score);
                    //calculate sum of total score
                    scoreTotal+=score;
                    viewCount = Double.valueOf(score_view.substring(idx + 1));
                    //adding viewCount in arraylist
                    ViewList.add(viewCount);
                    //calculate sum of total viewCount
                    viewCountTotal+=viewCount;
                }

            }

            double viewCountAvg=viewCountTotal/ViewList.size();
            double scoreAvg=scoreTotal/ViewList.size();


            System.out.println("length of arraylist "+ScoreList.size());
            System.out.println("viewCountAvg "+ viewCountAvg);
            System.out.println("scoreAvg "+ scoreAvg);

            double corr=0.0;
            corr = scoreViewCor(ScoreList,ViewList,scoreAvg,viewCountAvg);
            System.out.println("corr "+ corr);

            result.set(corr);
            resultKey.set("sample correlation coefficient");

            context.write(resultKey, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();


        Job job = Job.getInstance(conf, "Sample correlation coefficient");
        job.setJarByClass(Aggregation_TaskC.class);
        job.setMapperClass(Aggregation_TaskC.Map.class);
        job.setReducerClass(Aggregation_TaskC.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
        ToolRunner.run(new Configuration(),new Aggregation_TaskC(), args);
    }

}







