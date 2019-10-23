package cs435.hadoop.ProfileA;

import cs435.hadoop.ProfileA.ProfileADriver.DocumentsCount;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Reducer;

public class TF_IDFReducer extends Reducer<Text, Text, LongWritable, Text> {
  private long mapperCounter;
  private Map<String, Double> docIdTFvalue = new HashMap<>();

  @Override
  public void setup(Context context) throws IOException, InterruptedException{
    Configuration conf = context.getConfiguration();
    JobClient client = new JobClient((JobConf) conf);
    RunningJob parentJob =
        client.getJob((org.apache.hadoop.mapred.JobID) JobID.forName( conf.get("mapred.job.id") ));
    mapperCounter = parentJob.getCounters().getCounter(DocumentsCount.NUMDOCS);
  }

  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    docIdTFvalue.clear();

    //Input -- <unigram, docId;tfValue>

    double unigramArticleCount = 0.0;
    for(Text value: values){
      String[] docInfo = value.toString().split(";");
      unigramArticleCount += 1;
    }

    double idf = Math.log10(mapperCounter / unigramArticleCount );

    context.write(new LongWritable(mapperCounter), new Text(""));
  }
}
