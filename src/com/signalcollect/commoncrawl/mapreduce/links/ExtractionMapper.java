package com.signalcollect.commoncrawl.mapreduce.links;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
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

public class ExtractionMapper extends MapReduceBase implements
		Mapper<Text, ArcFileItem, LongWritable, ArrayList<LongWritable>> {

	public void map(Text key, ArcFileItem value,
			OutputCollector<LongWritable, ArrayList<LongWritable>> output, Reporter reporter)
			throws IOException {
		try {
			if (!value.getMimeType().contains("text")) {
				return; // Only parse text.
			}
			
			ByteArrayInputStream inputStream = new ByteArrayInputStream(value
					.getContent().getReadOnlyBytes(), 0, value.getContent()
					.getCount());

			
			String sourceURI = value.getUri();
			String content = new Scanner(inputStream).useDelimiter("\\A").next();
			Document doc = Jsoup.parse(content);
			doc.setBaseUri(sourceURI);
			
			System.out.println("base: " + sourceURI);

			
			Elements links = doc.select("a[href]");
			for (Element link : links) {
				System.out.println("link to: " + link.absUrl("href"));
				ArrayList<LongWritable> linkWrapper = new ArrayList<LongWritable>();
				linkWrapper.add(new LongWritable(link.absUrl("href").hashCode()));
			    output.collect(new LongWritable(sourceURI.hashCode()), linkWrapper);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}