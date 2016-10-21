package code.articles;

import java.io.File;
import java.io.IOException;
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

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
      		//loading people file
			String PEOPLE_FILE = "people.txt";
	    	ClassLoader cl = GetArticlesMapred.class.getClassLoader();
	    	String fileUrl = cl.getResource(PEOPLE_FILE).getFile();
	    	
	    	// Get jar path and then scan
	    	String jarUrl = fileUrl.substring(5, fileUrl.length() - PEOPLE_FILE.length() - 2);
	    	JarFile jf = new JarFile(new File(jarUrl));
	    	Scanner sc = new Scanner(jf.getInputStream(jf.getEntry(PEOPLE_FILE)));
	    	
	    	//read names into HashSet one line at a time
	    	names = new HashSet<String>();
			String scanName = "";
			while(sc.hasNextLine()){
				scanName = sc.nextLine();
				names.add(scanName);
			}
			sc.close();
			super.setup(context);
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			context.getCounter(Records.TOTAL).increment(1);
			ArrayList<String> stringWindow = new ArrayList<String>();
		  	StringTokenizer itr = new StringTokenizer(inputPage.getTitle());
		  	String nextToken = null;
		  	String window = "";
		  	Boolean found = false;
	      
	      	while (itr.hasMoreTokens()) {
	    		  	nextToken = itr.nextToken();
	    		  	stringWindow.add(nextToken);
    	  	}
	      
	      	while(!stringWindow.isEmpty()){
		      	for(int i = 0; i < stringWindow.size(); i++){
				  	if(i == 0){
					  	window = stringWindow.get(i);
					  	if((window.length() > 0 && !Character.isUpperCase(window.charAt(0))) && 
							  	(window.length() > 1 && !Character.isUpperCase(window.charAt(1))) &&
							  	(window.length() > 2 && !Character.isUpperCase(window.charAt(2))) ){
						  	i = stringWindow.size();
					  	} else {
						  	if(names.contains(window)){
							  	found = true;
						  	}
						  	if(window.length() > 1){
							  	if(names.contains(window.substring(0,window.length() - 1))){
								  	found = true;
							  	}
						  	}
						  	if(window.length() > 2){
							  	if(names.contains(window.substring(1,window.length() - 1))){
								  	found = true;
							  	}
						  	}
						  	if(window.length() > 4){
							  	if(names.contains(window.substring(2,window.length() - 2))){
								  	found = true;
							  	}
						  	}
					  	}
				  	} else {
					  	window = window + " " + stringWindow.get(i);
					  	if(names.contains(window)){
						  	found = true;
					  	}
					  	if(window.length() > 1){
						  	if(names.contains(window.substring(0,window.length() - 1))){
							  	found = true;
						  	}
					  	}
					  	if(window.length() > 2){
						  	if(names.contains(window.substring(1,window.length() - 1))){
							  	found = true;
						  	}
					  	}
					  	if(window.length() > 4){
						  	if(names.contains(window.substring(2,window.length() - 2))){
							  	found = true;
						  	}
					  	}
				  	}
			  	}
    		  	window = "";
    		  	stringWindow.remove(0);
	      	}

	      	if(found){
	      		String title = inputPage.getTitle() + ";";
	      		if (title != null) {
	          		docTitle.set(title);
	          		String content = inputPage.getContent().trim();
	          		content = content.replaceAll("\\n", " ");
	          		Text contentTxt = new Text(content);
	          		context.write(docTitle, contentTxt);
	      		}
	      	}
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
