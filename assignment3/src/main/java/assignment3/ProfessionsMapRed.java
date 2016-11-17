package assignment3;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

public class ProfessionsMapRed {
	
	public static class GetVocabMapper extends Mapper<LongWritable, Text, Text, StringIntegerList> {
		// map of professions to individuals
		public static HashMap<String,String> professions = new HashMap<String,String>();
		// set of individuals
		public static HashSet<String> people = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text,
				StringIntegerList>.Context context) throws IOException, InterruptedException {

			try {	
				// prepare the people file
				String PEOPLE_FILE = "people.txt";
				ClassLoader cl = GetVocabMapper.class.getClassLoader();
				String fileUrl = cl.getResource(PEOPLE_FILE).getFile();
				
				// Get jar path
				String jarUrl = fileUrl.substring(5, fileUrl.length() - PEOPLE_FILE.length() - 2);
				JarFile jf = new JarFile(new File(jarUrl));
				// Scan the people.txt file inside jar
				Scanner sc = new Scanner(jf.getInputStream(jf.getEntry(PEOPLE_FILE)));
				
				// loop through the scanner
				while (sc.hasNextLine())
				{
					String name = sc.nextLine();
					// store this individual
					people.add(name);				
				}
				
				// close the scanner stream and jar file
				sc.close();
				jf.close();
				
				// prepare the professions file
				String PROFESSIONS_FILE = "professions.txt";
				cl = GetVocabMapper.class.getClassLoader();
				fileUrl = cl.getResource(PROFESSIONS_FILE).getFile();
				
				// Get jar path
				jarUrl = fileUrl.substring(5, fileUrl.length() - PROFESSIONS_FILE.length() - 2);
				jf = new JarFile(new File(jarUrl));
				// Scan the people.txt file inside jar
				sc = new Scanner(jf.getInputStream(jf.getEntry(PROFESSIONS_FILE)));
				
				// loop until there are no more lines in the stream
				while (sc.hasNextLine())
				{
					String line = sc.nextLine();
					String[] nameProf = line.split(" : ", 2);
					
					// only work with a properly-formatted professions line
					if (nameProf.length == 2) {
						// individual's name is the first element
						String person = nameProf[0];
						// do we know that person?  (are they in the set?)
						if (people.contains(person)){
							
							String[] professionsList = nameProf[1].split(",");
							// map this individual to the first profession in the list.
							// This is done rather than using every profession in
							// order to avoid issues with lemma prior probabilities.
							String prof = professionsList[0];
							professions.put(person, prof);
						}
					}
				}
				
				// tidy up by closing the scanner stream and jar file
				sc.close();
				jf.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				// We'd like to know what happened here.
				e.printStackTrace();
			}
			// run setup of the mapper superclass
			super.setup(context);
		}

		@Override
		public void map(LongWritable articleId, Text indices, Context context) throws IOException, InterruptedException {
			
			// find the individual's name
			String indicesStr = indices.toString();
			int index = indicesStr.indexOf("<");
			if (index == -1){
				// bad input! stop this mapping process
				return;
			}
			String name = indicesStr.substring(0, index);
			name = name.trim();
			
			// look this individual up in the professions hashmap
			String profession = professions.get(name);
			
			if (profession != null) {
				// read in pairs 
				StringIntegerList indicesSIL = new StringIntegerList();
				indicesSIL.readFromString(indicesStr);
				
				// write the profession and indices to output
				context.write(new Text(profession), indicesSIL);
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		// configure and set up
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		// the only arguments should be the paths for input and output directories
		if (otherArgs.length != 2) {
			System.err.println("Incorrect arguments");
			System.exit(2);
		}
		// start the job
		Job job = Job.getInstance(conf);
		job.setJarByClass(ProfessionsMapRed.class);
		// set up mapper class
		job.setMapperClass(GetVocabMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringIntegerList.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntegerList.class);
		// reducer class isn't needed for this job
		// set the input and output directories
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// on finishing the job successfully, close
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
