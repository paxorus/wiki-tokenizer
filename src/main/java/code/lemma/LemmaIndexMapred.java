package code.lemma;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import code.articles.GetArticlesMapred;
import code.articles.GetArticlesMapred.GetArticlesMapper;
import util.StringIntegerList;
import util.WikipediaPageInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, Text, Text, StringIntegerList> {

		@Override
		public void map(LongWritable offset, Text text, Context context) throws IOException,
				InterruptedException {
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] stringArgs = gop.getRemainingArgs();

	    Job job = Job.getInstance(conf, "LemmaIndexMapred");
	    job.setJarByClass(LemmaIndexMapred.class);
	    job.setMapperClass(LemmaIndexMapper.class);
	    job.setInputFormatClass(TextInputFormat.class);
    	//job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    //job.setOutputValueClass(WikipediaPageFactory.getWikipediaPageClass(language));
	    //Version 1.1.1 does not have WikipediaPAgeFactory, introduced in 2.0
	    job.setOutputValueClass(StringIntegerList.class);
	    TextInputFormat.addInputPath(job, new Path(stringArgs[0])); 
	    FileOutputFormat.setOutputPath(job, new Path(stringArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
