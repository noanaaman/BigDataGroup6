package assignment3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

public class CreateVectors {
	
	private static Integer wordCount = 1;
	private static final Map<String, Integer> vocab = Maps.newHashMap();
	private String indexPath;
	
	public CreateVectors(String indexPath)
	{
		this.indexPath = indexPath;
	}
	
	public List<MahoutVector> vectorize() throws IOException {
		return vectorize(this.indexPath);
	}
	
	public List<MahoutVector> vectorize(String indexPath) throws IOException {
		
		List<MahoutVector> vectors = Lists.newArrayList();
		BufferedReader br = new BufferedReader(new FileReader(indexPath));
		try {
			String line = br.readLine();
			
			while (line != null) {
				Vector vector = new RandomAccessSparseVector(line.length()-1, line.length()-1);
				
				String[] profIndex = line.split("\t");
				String profession = profIndex[0];
				
				StringIntegerList indicesSIL = new StringIntegerList();
				indicesSIL.readFromString(profIndex[1]);
				
				for (StringInteger si: indicesSIL.getIndices()) {
					vector.set(processString(si.getString()),(double)si.getValue());
				}
				
				MahoutVector mahoutVector = new MahoutVector();
				mahoutVector.setClassifier(profession); 
				mahoutVector.setVector(vector);
				vectors.add(mahoutVector);
				
			}
			return vectors;
			
		} finally {
			br.close();
		}
		
	}
	
	
	public void createSeqFile() throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);
		Path seqFilePath = new Path("????");
		
		fs.delete(seqFilePath,false);
		
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, seqFilePath, Text.class, VectorWritable.class);

		String indexPath = "???";

		try
		{
			
			List<MahoutVector> vectors = vectorize(indexPath);

			
			// Init the labels
			
			for (MahoutVector vector : vectors)
			{
				VectorWritable vectorWritable = new VectorWritable();
				vectorWritable.set(vector.getVector());
				writer.append(new Text("/" + vector.getClassifier() + "/"), vectorWritable);
			}
		}

		finally
		{
			writer.close();
		}
	}
	
	
	protected int processString(String data)
	{
		
		Integer wordIndex = vocab.get(data);
		if (wordIndex == null)
		{
			wordIndex = wordCount++;
			vocab.put(data, wordIndex);
		}
		
		return wordIndex;
	}

	
	
}
