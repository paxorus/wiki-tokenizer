package code.lemma;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
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

		private Tokenizer tokenizer;
		
		@Override
	    public void setup(Context context) throws IOException {
	    	tokenizer = new Tokenizer();
	    }
	    
		@Override
		public void map(LongWritable offset, Text text, Context context) throws IOException,
				InterruptedException {
			// parse
			String article = text.toString();
			int idx = article.indexOf(";\t");
			Text title = new Text(article.substring(0, idx));
			String body = article.substring(idx + 2);
			
			// tokenize and count
			List<String> tokens = tokenizer.process(body);
			StringIntegerList freqList = count(tokens);
			context.write(title, freqList);
		}
		
		public StringIntegerList count(List<String> tokens) {
			Map<String, Integer> freq = new HashMap<String, Integer>();
			for (String token : tokens) {
				Integer count = freq.get(token);
				if (count != null) {
					freq.put(token, count.intValue() + 1);
				} else {
					freq.put(token, 1);
				}
			}
			
			return new StringIntegerList(freq);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] stringArgs = gop.getRemainingArgs();

	    Job job = Job.getInstance(conf, "lemma-indexer");
	    job.setJarByClass(LemmaIndexMapred.class);
	    job.setMapperClass(LemmaIndexMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(StringIntegerList.class);
	    
	    TextInputFormat.addInputPath(job, new Path(stringArgs[0])); 
	    FileOutputFormat.setOutputPath(job, new Path(stringArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
