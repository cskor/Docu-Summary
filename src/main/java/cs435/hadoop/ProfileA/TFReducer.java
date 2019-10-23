package cs435.hadoop.ProfileA;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
  private Map<String, Double> freqCounts = new HashMap<>();

  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    freqCounts.clear();
    long maxFreq = -1;

    //Find the max_k f_kj
    for(Text value: values){
      String[] freqInfo = value.toString().split(";");
      freqCounts.put(freqInfo[0], Double.parseDouble(freqInfo[1]));

      if(Long.parseLong(freqInfo[1]) > maxFreq)
        maxFreq = Long.parseLong(freqInfo[1]);
    }

    double tf;
    for(String unigram: freqCounts.keySet()){
      tf = 0.5 + (0.5 * ( freqCounts.get(unigram) / maxFreq));
      context.write(key, new Text(unigram + ";" + tf));
    }
  }
}
