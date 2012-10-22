package com.signalcollect.commoncrawl.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ExtractionCombiner extends MapReduceBase implements
		Reducer<LongWritable, LongWritable, LongWritable, Set<LongWritable>> {
	public void reduce(LongWritable sourceId, Iterator<LongWritable> values,
			OutputCollector<LongWritable, Set<LongWritable>> output, Reporter reporter)
			throws IOException {

		Set<LongWritable> links = new HashSet<LongWritable>();
		Iterator<LongWritable> it = values;
		while (it.hasNext()) {
			links.add(it.next());
		}
		output.collect(sourceId, links);
	}
}
