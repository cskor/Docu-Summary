package cs435.hadoop.ProfileA;

import cs435.hadoop.ProfileA.ProfileADriver.DocumentsCount;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TF_IDFMapper extends Mapper<LongWritable, Text, Text, Text> {
  private static Set<Long> documentID = new HashSet<>();

  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //Input -- <docId unigram;tf value>
    String[] input = value.toString().split("\\s+");
    String[] unigramInfo = input[1].split(";");

    if(documentID.add(Long.parseLong(input[0])))
      context.getCounter(DocumentsCount.NUMDOCS).increment(1);

    //Output --<unigram, docId;tfValue>
    context.write( new Text(unigramInfo[0]), new Text(input[0] + ";" + unigramInfo[1]));
  }
}
