package cs435.hadoop.ProfileB;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentenceDriver {
  public static void main(String[] args){
    try{
      Configuration conf = new Configuration();

      Job job = Job.getInstance(conf, "Sentence Summary");
      job.setJarByClass(cs435.hadoop.ProfileB.SentenceDriver.class);

      MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CacheMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SentenceMapper.class);
      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      job.setReducerClass(SentenceReducer.class);
      job.setNumReduceTasks(10);

      // Outputs from the Mapper.
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // Block until the job is completed.
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }

}
