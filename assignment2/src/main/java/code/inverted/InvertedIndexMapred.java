package code.inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
/* IN Dogg_Catt <woof, 3> <meow, 1>
* 	  Catt_Meow <woof, 1>
* OUT woof <Dogg_Woof, 3> <Catt_Meow, 1>
*/

public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, StringInteger> {

		@Override
		public void map(LongWritable articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here

//			Pattern p = Pattern.compile("<(.*?)>");
//			Matcher m = p.matcher(indices.toString());
//			List<String> allMatches = new ArrayList<String>();
//			 while (m.find()) {
//			   allMatches.add(m.group(0));
//			 }
//
//			 for (String out:allMatches){			 
//				 String indicesStr = indices.toString();
//				 String name = indicesStr.substring(0, indicesStr.indexOf("<"));
//				 name = name.trim();
//				 
//				 out = out.replace("<", " ");
//				 out = out.replace(">", " ");
//				 out = out.replace(",", " ");
//				 String[] pieces = out.split("\\s+");
//				 
//				 context.write(new Text(pieces[1]), new StringInteger(name, Integer.parseInt(pieces[2])));
//			 }
			
			String indicesStr = indices.toString();
			String name = indicesStr.substring(0, indicesStr.indexOf("<"));
			name = name.trim();
			StringIntegerList indicesSIL = new StringIntegerList();
			indicesSIL.readFromString(indicesStr);
			
			for (StringInteger si: indicesSIL.getIndices()){
					context.write(new Text(si.getString()), new StringInteger(name, si.getValue()));
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement inverted index reducer here
			System.out.println(lemma);
			List<StringInteger> copy = new ArrayList<StringInteger>();
			for (StringInteger si:articlesAndFreqs){
			    System.out.println(si);
				copy.add(si);
			}
			context.write(lemma, new StringIntegerList(copy));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Incorrect arguments");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(InvertedIndexMapred.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringInteger.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntegerList.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
