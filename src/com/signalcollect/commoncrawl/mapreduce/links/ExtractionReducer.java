package com.signalcollect.commoncrawl.mapreduce.links;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ExtractionReducer extends MapReduceBase implements
		Reducer<LongWritable, ArrayList<LongWritable>, LongWritable, ArrayList<LongWritable>> {
	public void reduce(LongWritable sourceId, Iterator<ArrayList<LongWritable>> values,
			OutputCollector<LongWritable, ArrayList<LongWritable>> output, Reporter reporter)
			throws IOException {

		ArrayList<LongWritable> links = new ArrayList<LongWritable>();
		Iterator<ArrayList<LongWritable>> it = values;
		while (it.hasNext()) {
			links.addAll(it.next());
		}
		output.collect(sourceId, links);
	}
}
