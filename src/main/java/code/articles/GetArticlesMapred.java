package code.articles;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.WikipediaPageInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author Tuan
	 *
	 */
	//@formatter:on
	private static enum Records { TOTAL };

	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		
		public static Set<String> names = new HashSet<String>();
		private static final Text docTitle = new Text();
		private static final String PEOPLE_FILE = "people.txt";

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context) throws IOException, InterruptedException {
      		//loading people file
	    	ClassLoader cl = GetArticlesMapred.class.getClassLoader();
	    	BufferedReader reader = new BufferedReader(new InputStreamReader(cl.getResourceAsStream(PEOPLE_FILE)));
	    	
	    	//read names into HashSet one line at a time
	    	names = new HashSet<String>();
			String scanName;
			while((scanName = reader.readLine()) != null){
				names.add(scanName);
			}
			reader.close();
			super.setup(context);
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context) throws IOException, InterruptedException {

      		String title = inputPage.getTitle();// Albert Gore Loves Beagles
	      
	      	if (title == null) {
	      		return;
	      	}
	      	if (!names.contains(title)) {
	      		return;
	      	}
      		
      		String content;
      		try {
      			content = inputPage.getContent().trim();
      		} catch (NullPointerException npe) {
      			System.err.println("\tCloud9 fails on:" + title);
      			return;
      		}
      		Text body = new Text(content.replaceAll("\\n", " "));
      		docTitle.set(title + ";");
      		context.write(docTitle, body);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] stringArgs = gop.getRemainingArgs();

	    Job job = Job.getInstance(conf, "GetArticlesMapred");
	    job.setJarByClass(GetArticlesMapred.class);
	    job.setMapperClass(GetArticlesMapper.class);
	    job.setInputFormatClass(WikipediaPageInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    WikipediaPageInputFormat.addInputPath(job, new Path(stringArgs[0])); 
	    FileOutputFormat.setOutputPath(job, new Path(stringArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
