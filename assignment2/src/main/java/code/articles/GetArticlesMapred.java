package code.articles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, LongWritable, WikipediaPage> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, LongWritable, WikipediaPage>.Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DistributedCache here
			try {	
				BufferedReader br = new BufferedReader(new FileReader("people.txt"));
				String name = br.readLine();
				while (name != null)
				{
					peopleArticlesTitles.add(name);
					name = br.readLine();
					
				}
				
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.setup(context);
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
			// inputPage is a cloud9 wikipedia page
			String title = inputPage.getTitle();
			if (peopleArticlesTitles.contains(title)){
				context.write(offset,inputPage);
			}
			
			
			
		}
	}

	public static void main(String[] args) {
		// TODO: you should implement the Job Configuration and Job call
		// here
	}
}
