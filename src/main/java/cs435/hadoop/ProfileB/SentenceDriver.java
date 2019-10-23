package cs435.hadoop.ProfileB;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentenceDriver {
  public static void main(String[] args){
    try{
      Configuration conf = new Configuration();

      Job job = Job.getInstance(conf, "Sentence Summary");
      job.setJarByClass(cs435.hadoop.ProfileB.SentenceDriver.class);
      job.setMapperClass(SentenceMapper.class);

      //Try to load the theta values into the distributed cache
      DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());

      // Outputs from the Mapper.
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(NullWritable.class);

      // path to input in HDFS
      FileInputFormat.addInputPath(job, new Path(args[1]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      // Block until the job is completed.
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }

}
