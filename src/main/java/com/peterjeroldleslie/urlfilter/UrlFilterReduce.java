package com.peterjeroldleslie.urlfilter;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UrlFilterReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  private Text outValue = new Text("");

  public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    while (value.hasNext()) {
      URL url = new URL(value.next().toString());
      //Checks url that contains word 'contact'
      if (url.getPath().contains("contact")) {
        output.collect(key, outValue);
        break;
      }
    }
  }
}
