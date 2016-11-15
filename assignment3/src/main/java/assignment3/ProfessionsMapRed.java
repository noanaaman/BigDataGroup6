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
		public static HashMap<String,String> professions = new HashMap<String,String>();
		public static HashSet<String> people = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, StringIntegerList>.Context context) throws IOException, InterruptedException {

			try {	
				String PEOPLE_FILE = "people.txt";
				ClassLoader cl = GetVocabMapper.class.getClassLoader();
				String fileUrl = cl.getResource(PEOPLE_FILE).getFile();
				
				// Get jar path
				String jarUrl = fileUrl.substring(5, fileUrl.length() - PEOPLE_FILE.length() - 2);
				JarFile jf = new JarFile(new File(jarUrl));
				// Scan the people.txt file inside jar
				Scanner sc = new Scanner(jf.getInputStream(jf.getEntry(PEOPLE_FILE)));
				
				while (sc.hasNextLine())
				{
					String name = sc.nextLine();
					people.add(name);				
				}
				
				sc.close();
				jf.close();
				
				String PROFESSIONS_FILE = "professions.txt";
				cl = GetVocabMapper.class.getClassLoader();
				fileUrl = cl.getResource(PROFESSIONS_FILE).getFile();
				
				// Get jar path
				jarUrl = fileUrl.substring(5, fileUrl.length() - PROFESSIONS_FILE.length() - 2);
				jf = new JarFile(new File(jarUrl));
				// Scan the people.txt file inside jar
				sc = new Scanner(jf.getInputStream(jf.getEntry(PROFESSIONS_FILE)));
				
				while (sc.hasNextLine())
				{
					String line = sc.nextLine();
					String[] nameProf = line.split(" : ", 2);
					if (nameProf.length == 2) {
						String person = nameProf[0];
						if (people.contains(person)){
							String[] professionsList = nameProf[1].split(",");
							String prof = professionsList[0];
							
							professions.put(person, prof);
						}
						
					}
				}
				
				sc.close();
				jf.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.setup(context);
		}

		@Override
		public void map(LongWritable articleId, Text indices, Context context) throws IOException, InterruptedException {
			
			String indicesStr = indices.toString();
			int index = indicesStr.indexOf("<");
			if (index == -1){
				return;
			}
			String name = indicesStr.substring(0, index);
			name = name.trim();
			String profession = professions.get(name);
			StringIntegerList indicesSIL = new StringIntegerList();
			indicesSIL.readFromString(indicesStr);
			
			context.write(new Text(profession), indicesSIL);
			
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
		job.setJarByClass(ProfessionsMapRed.class);
		
		job.setMapperClass(GetVocabMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringInteger.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringInteger.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
