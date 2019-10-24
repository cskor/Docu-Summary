package cs435.hadoop.ProfileB;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

class UnigramCompare implements Comparator<String>{
  @Override
  public int compare(String first, String second) {
    //Input word;tf-idf value
    String[] firstParts = first.split(";");
    String[] secondParts = second.split(";");
    return -1 * Double.compare(Double.parseDouble(firstParts[1]), Double.parseDouble(secondParts[1]));
  }
}

class SentenceCompare implements Comparator<String>{
  @Override
  public int compare(String first, String second) {
    //Input word;tf-idf value
    String[] firstParts = first.split(";");
    String[] secondParts = second.split(";");
    return Double.compare(Double.parseDouble(firstParts[0]), Double.parseDouble(secondParts[0]));
  }
}

public class SentenceReducer extends Reducer<Text, Text, Text, Text> {

  private Map<String, String> unigramValues = new HashMap<>();
  private ArrayList<String> bestUnigramScores = new ArrayList<>();
  private ArrayList<String> bestSentenceScores = new ArrayList<>();
  private ArrayList<String> top3Sentences = new ArrayList<>();

  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      unigramValues.clear();
      bestSentenceScores.clear();
      top3Sentences.clear();

      String article = "";

      for (Text value : values) {
        if (value.charAt(0) == 'S')
          article = value.toString().substring(1);
        else {
          String input = value.toString().substring(1);
          String[] unigramInfo = input.split(";");
          unigramValues.put(unigramInfo[0], input);
        }
      }

      String[] sentences = article.split("\\. ");
      double total;
      int count;
      for (int i = 0; i < sentences.length; i++) {
        bestUnigramScores.clear();

        for (String word : sentences[i].split("\\s+")) {
          word = word.toLowerCase().replaceAll("[^A-Za-z0-9]", "");
          if (word.length() > 0 && !bestUnigramScores.contains(unigramValues.get(word))){
            bestUnigramScores.add(unigramValues.get(word));
          }
        }

        //Sum up the sentence tf idf score
        total = 0;
        count = 0;
        bestUnigramScores.remove(null);
        bestUnigramScores.sort(new UnigramCompare());
        for (String bestUnigramScore : bestUnigramScores) {
          if (count == 5)
            break;
          total += Double.parseDouble(bestUnigramScore.split(";")[1]);
          count++;
        }

      bestSentenceScores.add("" + i + ";" + total);
      }

      bestSentenceScores.remove(null);
      bestSentenceScores.sort(new UnigramCompare());
      count = 0;
      //Keep only the top three
      for (String bestSentence : bestSentenceScores) {
        if( count == 3 )
          break;
        top3Sentences.add(bestSentence);
        count ++;
      }

      top3Sentences.remove(null);
      top3Sentences.sort( new SentenceCompare());
      StringBuilder finalSentence = new StringBuilder();

      for(String sent: top3Sentences){
        int index = Integer.parseInt(sent.split(";")[0]);
        finalSentence.append(sentences[index]);
        finalSentence.append(". ");
      }

      context.write(key, new Text(finalSentence.toString()));
  }
}
