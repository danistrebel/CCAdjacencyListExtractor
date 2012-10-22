package com.signalcollect.commoncrawl.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.util.Progressable;

import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;

public class ExtractionJob {

	/**
	 * Contains the Amazon S3 bucket holding the CommonCrawl corpus.
	 */
	private static final String CC_BUCKET = "aws-publicdatasets";

	/**
	 * Outputs counted words into a CSV-formatted file with the following
	 * structure for each source page:
	 * 
	 * SOURCE_PAGE_ID,NUMBER_OF_OUTGOING_LINKS,TARGET_ID_0,TARGET_ID_1,....,
	 * TARGET_ID_n
	 */
	public static class CSVOutputFormat extends
			TextOutputFormat<LongWritable, Set<LongWritable>> {
		public RecordWriter<LongWritable, Set<LongWritable>> getRecordWriter(
				FileSystem ignored, JobConf job, String name,
				Progressable progress) throws IOException {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new CSVRecordWriter(fileOut);
		}

		protected static class CSVRecordWriter implements
				RecordWriter<LongWritable, Set<LongWritable>> {
			protected DataOutputStream outStream;

			public CSVRecordWriter(DataOutputStream out) {
				this.outStream = out;
			}

			public synchronized void write(LongWritable key,
					Set<LongWritable> links) throws IOException {
				CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);
				csvOutput.writeLong(key.get(), "sourceId");
				csvOutput.writeInt(links.size(), "number of links");
				for (LongWritable linkId : links) {
					csvOutput.writeLong(linkId.get(), "link");
				}
			}

			public synchronized void close(Reporter reporter)
					throws IOException {
				outStream.close();
			}
		}
	}

	/**
	 * Executes an AWS Elastic Map Reduce job to extract the adjacency list from
	 * the common crawl data set.
	 * 
	 * @param args
	 *            [0] = aws secret key ID
	 * @param args
	 *            [1] = aws secret key
	 * @param args
	 *            [2] = Path suffix of craws to process
	 * @param args
	 *            [3] = S3 bucket and path of the output directory
	 */
	public static void main(String[] args) {
		// Parse command-line arguments.
		String awsCredentials = args[0];
		String awsSecret = args[1];
		String inputPrefixes = "common-crawl/crawl-002/" + args[2];
		String outputFile = args[3];

		// Configure a new AWS Hadoop job.
		JobConf conf = new JobConf();
		conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
		conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
		conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
		conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);

		// Configures where the input comes from when running our Hadoop job,
		// in this case, gzipped ARC files from the specified Amazon S3 bucket
		// paths.
		ARCInputFormat.setARCSourceClass(conf, JetS3tARCSource.class);
		ARCInputFormat inputFormat = new ARCInputFormat();
		inputFormat.configure(conf);
		conf.setInputFormat(ARCInputFormat.class);

		// Configures what kind of Hadoop output we want.
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Set.class);

		// Configures where the output goes to when running our Hadoop job.
		CSVOutputFormat.setOutputPath(conf, new Path(outputFile));
		CSVOutputFormat.setCompressOutput(conf, false);
		conf.setOutputFormat(CSVOutputFormat.class);

		// Allows some (5%) of tasks fail; we might encounter the
		// occasional troublesome set of records and skipping a few
		// of 1000s won't hurt counts too much.
		conf.set("mapred.max.map.failures.percent", "5");

		// Tests if inputs are available
		InputSplit[] splits;
		try {
			splits = inputFormat.getSplits(conf, 0);
			if (splits.length == 0) {
				System.out.println("ERROR: No .ARC files found!");
				return;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Tells Hadoop what Mapper and Reducer classes to use;
		conf.setMapperClass(ExtractionMapper.class);
		conf.setCombinerClass(ExtractionCombiner.class);
		conf.setReducerClass(ExtractionReducer.class);

		// Tells Hadoop mappers and reducers to pull dependent libraries from
		// those bundled into this JAR.
		conf.setJarByClass(ExtractionJob.class);

		System.out.println("Started Job");
		// Runs the job.
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Job done");
	}
}
