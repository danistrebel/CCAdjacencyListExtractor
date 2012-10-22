package com.signalcollect.commoncrawl.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.Set;

public class ExtractionReducer extends MapReduceBase implements
		Reducer<LongWritable, Set<LongWritable>, LongWritable, Set<LongWritable>> {
	public void reduce(LongWritable sourceId, Iterator<Set<LongWritable>> values,
			OutputCollector<LongWritable, Set<LongWritable>> output, Reporter reporter)
			throws IOException {

		Set<LongWritable> links = new HashSet<LongWritable>();
		Iterator<Set<LongWritable>> it = values;
		while (it.hasNext()) {
			links.addAll(it.next());
		}
		output.collect(sourceId, links);
	}
}
