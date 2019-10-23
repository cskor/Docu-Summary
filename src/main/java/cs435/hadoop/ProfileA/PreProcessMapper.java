package cs435.hadoop.ProfileA;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreProcessMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
    //Parse the input String
    String[] words = value.toString().split("<====>");

    //Do not want empty lines or empty texts
    if(words.length != 3 )
      return;

    if(words[2].length() == 0)
      return;

    for(String word: words[2].split("\\s+")){
      word = word.toLowerCase().replaceAll("[^A-Za-z0-9]", "");
      if(word.length() > 0)
        context.write(new Text(words[1] + ";" + word), new IntWritable(1));
    }
  }
}
