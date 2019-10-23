package cs435.hadoop.ProfileB;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import static java.util.stream.Collectors.*;

import org.apache.hadoop.mapreduce.Reducer;

public class SentenceReducer extends Reducer<Text, Text, Text, Text> {
  private Map<String, Double> unigramValues = new HashMap<>();
  private TreeMap<String, Double> bestUnigramScores = new TreeMap<>();
  private TreeMap<String, Double> sortedUnigram = new TreeMap<>();

  private TreeMap<Integer, Double> bestSentenceScores = new TreeMap<>();
  private TreeMap<Integer, Double> sortedSentences = new TreeMap<>();

  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    unigramValues.clear();
    bestSentenceScores.clear();

    String article = "";

    for(Text value: values){
      if(value.charAt(0) == 'S')
        article = value.toString().substring(1);
      else{
        String input = value.toString().substring(1);
        String[] unigramInfo = input.split(";");
        unigramValues.put( unigramInfo[0], Double.parseDouble(unigramInfo[1]));
      }
    }

    String[] sentences = article.split("\\. ");
    double total;

    for(int i = 0; i < sentences.length; i++){
      bestUnigramScores.clear();

      for(String word: sentences[i].split("\\s+")){
        word = word.toLowerCase().replaceAll("[^A-Za-z0-9]", "");
        if(word.length() > 0 && !bestUnigramScores.containsKey(word)){
            bestUnigramScores.put(word, unigramValues.get(word));

            if(bestUnigramScores.size() > 5) {
              //Sort the tree map by value
              sortedUnigram = bestUnigramScores.entrySet()
                  .stream()
                  .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                  .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, TreeMap::new));
              bestUnigramScores.remove(sortedUnigram.lastKey());
            }
          }
        }

      //Sum up the sentence tf idf score
      total = 0;
      for(String unigram: bestUnigramScores.keySet()) {
        total += bestUnigramScores.get(unigram);
      }
      bestSentenceScores.put(i, total);

      if(bestSentenceScores.size() > 3) {
        //Sort the tree map by value
        sortedSentences = bestSentenceScores.entrySet()
            .stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, TreeMap::new));
        bestSentenceScores.remove(sortedSentences.lastKey());
      }
    }

    for(Integer index: bestSentenceScores.keySet()){
      context.write(key, new Text( sentences[index]));
    }
  }
}
