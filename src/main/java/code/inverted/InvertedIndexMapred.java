package code.inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, StringInteger> {

		@Override
		public void map(LongWritable uselessCrap, Text text, Context context) throws IOException,
				InterruptedException {
			
			// articleId = title, indices = <beagle, 2> <puppy, 3>
			String raw = text.toString();
			int tabIdx = raw.indexOf("\t");
			String articleId = raw.substring(0, tabIdx);
			raw = raw.substring(tabIdx + 1);
			String[] pairs = raw.substring(1, raw.length() - 1).split(">,<");// ["beagle, 2", "puppy, 3"]
			for (String pair : pairs) {
				int idx = pair.lastIndexOf(",");
				Text token = new Text(pair.substring(0, idx));// beagle
				int count = Integer.parseInt(pair.substring(idx + 1), 10);// 2
				StringInteger sint = new StringInteger(articleId, count);

				// write(puppy, <articleId, 2>)
				context.write(token, sint);
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// for token beagle, receive Iterable<articleId, count>
			List<StringInteger> vector = new ArrayList<StringInteger>();
			for (StringInteger sint : articlesAndFreqs) {
				sint = new StringInteger(sint.getString(), sint.getValue());
				vector.add(sint);
			}
			StringIntegerList siList = new StringIntegerList(vector);
			context.write(lemma, siList);			
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    GenericOptionsParser gop = new GenericOptionsParser(conf, args);
	    String[] otherArgs = gop.getRemainingArgs();
	    
	    Job job = Job.getInstance(conf, "inverter");
	    
	    job.setJarByClass(InvertedIndexMapred.class);
	    job.setMapperClass(InvertedIndexMapper.class);
	    // Setting the CombinerClass causes an issue with inconsistent types.
	    job.setReducerClass(InvertedIndexReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(StringIntegerList.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(StringInteger.class);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
