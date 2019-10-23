package cs435.hadoop.ProfileA;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfileADriver {

  public enum DocumentsCount{
    NUMDOCS
  }

  public static void main(String[] args){
    try {
      Configuration conf = new Configuration();

      /* JOB A -- PREPROCESS DATA*/
      Job jobA = Job.getInstance(conf, "Pre-Process Input Data");
      jobA.setJarByClass(cs435.hadoop.ProfileA.ProfileADriver.class);
      jobA.setMapperClass(PreProcessMapper.class);
      jobA.setReducerClass(PreProcessReducer.class);
      jobA.setCombinerClass(PreProcessReducer.class);
      jobA.setNumReduceTasks(10);

      // Outputs from the Mapper.
      jobA.setMapOutputKeyClass(Text.class);
      jobA.setMapOutputValueClass(IntWritable.class);

      //Set the outputs
      jobA.setOutputKeyClass(Text.class);
      jobA.setOutputValueClass(IntWritable.class);

      // path to input in HDFS
      FileInputFormat.addInputPath(jobA, new Path(args[0]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(jobA, new Path(args[1]));
      jobA.waitForCompletion(true);

      /* JOB B -- CALCULATE TF VALUES*/
      Job jobB = Job.getInstance(conf, "Calculate TF Values");
      jobB.setJarByClass(cs435.hadoop.ProfileA.ProfileADriver.class);
      jobB.setMapperClass(TFMapper.class);
      jobB.setReducerClass(TFReducer.class);
      jobB.setNumReduceTasks(10);

      // Outputs from the Mapper.
      jobB.setMapOutputKeyClass(LongWritable.class);
      jobB.setMapOutputValueClass(Text.class);

      //Set the outputs
      jobB.setOutputKeyClass(LongWritable.class);
      jobB.setOutputValueClass(Text.class);

      // path to input in HDFS
      FileInputFormat.addInputPath(jobB, new Path(args[1]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(jobB, new Path(args[2]));
      jobB.waitForCompletion(true);

      /* JOB C -- CALCULATE TF-IDF VALUES*/
      Job jobC = Job.getInstance(conf, "Calculate TF-IDF Values");
      jobC.setJarByClass(cs435.hadoop.ProfileA.ProfileADriver.class);
      jobC.setMapperClass(TF_IDFMapper.class);
      jobC.setReducerClass(TF_IDFReducer.class);
      jobC.setNumReduceTasks(10);

      // Outputs from the Mapper.
      jobC.setMapOutputKeyClass(Text.class);
      jobC.setMapOutputValueClass(Text.class);

      //Set the outputs
      jobC.setOutputKeyClass(LongWritable.class);
      jobC.setOutputValueClass(Text.class);

      // path to input in HDFS
      FileInputFormat.addInputPath(jobC, new Path(args[2]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(jobC, new Path(args[3]));

      // Block until the job is completed.
      System.exit(jobC.waitForCompletion(true) ? 0 : 1);

    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }
  }

}
