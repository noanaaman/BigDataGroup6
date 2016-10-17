package code.lemma;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import util.StringIntegerList;
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
			
			List<String> lemmas = Tokenizer.lemmatize(page);
			Text title = page.getTitle();
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
}
