package cs435.hadoop.ProfileB;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CacheMapper extends Mapper<LongWritable, Text, Text, Text> {
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] input = value.toString().split("\\s+");
    context.write(new Text(input[0]), new Text("C" + input[1]));
  }
}
