package com.peterjeroldleslie.urlfilter;

import java.io.IOException;
import java.net.URL;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.martinkl.warc.WARCWritable;

public class UrlFilterMap extends MapReduceBase implements Mapper<LongWritable, WARCWritable, Text, Text> {
  private Text    outKey   = new Text();
  private Text    outValue = new Text();
  //private Pattern p        = Pattern.compile("(http|https):\\/\\/[A-Za-z0-9\\.]*\\.(com|org|net)");

  public void map(LongWritable key, WARCWritable value, OutputCollector<Text, Text> collector, Reporter reporter)
      throws IOException {

    String targetURL = value.getRecord().getHeader().getTargetURI();

    if (targetURL != null) {
      URL url = new URL(targetURL);
      String host = url.getHost();
      String base = url.getProtocol() + "://" + host;

      //1.Checks and allow host that doesnt contain more than one special character
      //2.Checks and allow baseUrl that matches (*.com | *.org | *.net)
      if (base.matches(".*?\\.(com|org|net)") && !host.matches("^.*[._\'!#$%&*+\\\\/=?{|}~`\\^-]{2}.*$")) {
        outKey.set(base);
        outValue.set(targetURL);
        collector.collect(outKey, outValue);
      }
    }
  }
}
