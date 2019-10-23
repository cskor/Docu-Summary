package cs435.hadoop.ProfileA;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
    String[] freq = value.toString().split("\\s+");
    String[] wordInfo = freq[0].split(";");

    //Output -- <docId, word;freq>
    context.write(new LongWritable(Long.parseLong(wordInfo[0])), new Text(wordInfo[1] + ";" + freq[1]));
  }
}
