package code.lemma;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import code.inverted.InvertedIndexMapred;
import code.inverted.InvertedIndexMapred.InvertedIndexMapper;
import code.inverted.InvertedIndexMapred.InvertedIndexReducer;
import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import util.StringIntegerList.StringInteger;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException, InterruptedException {
			// TODO: implement Lemma Index mapper here
			
			// TODO:
			// 1. get the content of the input wikipedia page
			//    -- page.getContent() --> String of the text content of the page
			// 2. tokenize it using the Tokenizer class
			//    -- Tokenizer.tokenize(string) --> list of strings of tokens
			// 4. lemmatize
			// 5. output: Text Title, StringIntegerList lemmas_and_counts
			//    COMPLETE
			Tokenizer tokenizer = new Tokenizer();
			List<String> lemmas = tokenizer.getLemmas(page.getContent());
			Text title = new Text(page.getTitle());
			Map<String, Integer> frequencies = new HashMap<String,Integer>();
			// loop through all lemmas for this page
			for (String lemma : lemmas) {
				Integer f = frequencies.get(lemma);
				// check for null
				if (f == null) {
					f = new Integer(0);
				}
				// update or create the frequency for this lemma
				frequencies.put(lemma, f+1);
			}
			// convert the frequency map to a StringIntegerList ...
			List<StringInteger> lemmacounts = new ArrayList<StringInteger>();
			for (Map.Entry<String,Integer> e : frequencies.entrySet()) {
				StringInteger lemmacount = new StringInteger(e.getKey(), e.getValue());
				lemmacounts.add(lemmacount);
			}
			// ... and write it to the output
			context.write(title, new StringIntegerList(lemmacounts));
		}
	}
	
	// no Reducer is needed.
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Incorrect arguments");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(LemmaIndexMapred.class);
		job.setMapperClass(LemmaIndexMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntegerList.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
