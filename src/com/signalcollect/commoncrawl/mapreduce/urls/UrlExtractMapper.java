package com.signalcollect.commoncrawl.mapreduce.urls;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.commoncrawl.protocol.shared.ArcFileItem;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Outputs all words contained within the displayed text of pages contained
 * within {@code ArcFileItem} objects.
 * 
 * @author Steve Salevan <steve.salevan@gmail.com>
 */
public class UrlExtractMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, IntWritable> {

  public void map(Text key, ArcFileItem value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    try {
      if (!value.getMimeType().contains("text")) {
        return;  // Only parse text.
      }
      // Retrieves page content from the passed-in ArcFileItem.
      ByteArrayInputStream inputStream = new ByteArrayInputStream(
          value.getContent().getReadOnlyBytes(), 0,
          value.getContent().getCount());
      
      String baseUrl = value.getUri();
      output.collect(new Text(baseUrl), new IntWritable(1));
      
      
      // Converts InputStream to a String.
      String content = new Scanner(inputStream).useDelimiter("\\A").next();
      // Parses HTML with a tolerant parser and extracts all text.
      Document doc = Jsoup.parse(content, baseUrl);

      Elements links = doc.select("a[href]"); // a with href
      
      for (Element link : links) {
    	  output.collect(new Text(link.attr("abs:href")), new IntWritable(1));
      }
    }
    
    catch (Exception e) {
      reporter.getCounter("UrlExtractMapper.exception",
          e.getClass().getSimpleName()).increment(1);
    }
  }
}
