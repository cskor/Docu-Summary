package cs435.hadoop.ProfileB;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentenceMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //Parse the input String
      String[] words = value.toString().split("<====>");

      //Do not want empty lines or empty texts
      if(words.length != 3 )
        return;

      if(words[2].length() == 0)
        return;

      context.write(new Text(words[1]), new Text("S" + words[2]) );


  }
}
