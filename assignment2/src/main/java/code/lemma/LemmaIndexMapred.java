package code.lemma;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

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
		}
	}
}
