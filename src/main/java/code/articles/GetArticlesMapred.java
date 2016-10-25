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
		  	boolean found = searchTitle(title);
	      	if (!found ) {
	      		return;
	      	}
      		
      		String content;
      		try {
      			content = inputPage.getContent().trim();
      		} catch (NullPointerException npe) {
      			System.err.println("\tCloud9 errors on :" + title);
      			return;
      		}
      		inputPage.getContent().trim();
      		content = content.replaceAll("\\n", " ");
      		docTitle.set(title + ";");
      		Text contentTxt = new Text(content);
      		context.write(docTitle, contentTxt);
		}
		
		public static boolean searchTitle(String title) {
			
			if (names.contains(title)) {
				return true;
			}
		
		  	String[] tokens = title.split("\\s+");// [Albert, Gore, Loves, Beagles]
			
		  	// for Albert, Gore, Loves, Beagles
	      	for (int i = 0; i < tokens.length; i ++) {
	      		
	      		// choose a starting token
	      		String window = tokens[i];
	      		if (checkString(window)) {
	      			return true;
	      		}
	      		
	      		// first two characters aren't capital, skip it
	      		if ((window.length() > 0 && !Character.isUpperCase(window.charAt(0))) && 
			  	(window.length() > 1 && !Character.isUpperCase(window.charAt(1)))) {
	      			break;
	      		}
	      		
	      		// expand the window
	      		// for [Gore], [Gore Loves], [Gore, Loves, Beagles]
		      	for (int j = i + 1; j < tokens.length; j ++) {
		      		window += " " + tokens[j];
		      		if (checkString(window)) {
		      			return true;
		      		}
			  	}
	      	}
	      	return false;
		}
		
		public static boolean checkString(String window) {
			// "Albert Gore" case
		  	if(names.contains(window)){
			  	return true;
		  	}
		  	if (window.length() <= 2) {
		  		return false;
		  	}
		  	// "Albert Gore?" case
		  	if(names.contains(window.substring(0, window.length() - 1))){
			  	return true;
		  	}
		  	// "'Albert Gore'" case
		  	if(names.contains(window.substring(1, window.length() - 1))){
			  	return true;
		  	}
		  	return false;
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
	    //job.setOutputValueClass(WikipediaPageFactory.getWikipediaPageClass(language));
	    //Version 1.1.1 does not have WikipediaPAgeFactory, introduced in 2.0
	    job.setOutputValueClass(Text.class);
	    WikipediaPageInputFormat.addInputPath(job, new Path(stringArgs[0])); 
	    FileOutputFormat.setOutputPath(job, new Path(stringArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
