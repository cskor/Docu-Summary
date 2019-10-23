package cs435.hadoop.ProfileB;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class SentenceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
  //Key - docId;Unigram Value - TF-IDF score
  private Map<String, Double> unigramValues = new HashMap<>();

  private Path[] localFiles;
  private FileInputStream fis = null;
  BufferedInputStream bis = null;

  @Override
  public void setup(Context context) {
    //Read the distributed cache
    try {
      localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

      File directory = new File(localFiles[0].toString());
      File[] files = directory.listFiles();

      for (File file : files) {
        if (!file.toString().contains(".crc") && !file.toString().contains("SUCCESS")) {
          try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);

            BufferedReader d = new BufferedReader(new InputStreamReader(bis));
            String line = null;
            while ((line = d.readLine()) != null) {
              String[] input = line.split("\\s+");
              String[] unigramInfo = input[1].split(";");

              unigramValues.put("" + input[0] + ";" + unigramInfo[0], Double.parseDouble(unigramInfo[1]));
            }
          } catch (IOException e) { e.printStackTrace(); }
        }
      }
    } catch (IOException e) { e.printStackTrace(); }
  }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    for(String id: unigramValues.keySet()){
      context.write(new Text(id), NullWritable.get());
    }

  }
}
