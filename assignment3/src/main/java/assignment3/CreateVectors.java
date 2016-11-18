package assignment3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
	
	// initialize map of vocabulary and their indices
	private static Integer wordCount = 1;
	private static final Map<String, Integer> vocab = Maps.newHashMap();
	// path to source file
	private String indexPath;
	private HashSet<String> labels;
	
	// constructor needed
	public CreateVectors(String indexPath)
	{
		this.indexPath = indexPath;
		this.labels = new HashSet<String>();
	}
	
	
	
	// vectorizer with no arguments
	public List<MahoutVector> vectorize() throws IOException {
		return vectorize(this.indexPath);
	}
	
	// creates vectors for the given index file
	public List<MahoutVector> vectorize(String indexPath) throws IOException {
		
		// initialize vectors and file reader
		List<MahoutVector> vectors = Lists.newArrayList();
		BufferedReader br = new BufferedReader(new FileReader(indexPath));
		
		// read until nothing remains in the buffer
		try {
			String line = br.readLine();
			
			while (line != null) {
				
				// initialize a new sparse vector for this line with attributes:
				// cardinality: estimate of initialized sparseness
				// initial size: size of a double hashmap representing the vector
				Vector vector = new RandomAccessSparseVector(line.length()-1, line.length()-1);
				
				// split the line on tabs
				String[] profIndex = line.split("\t");
				// store the profession as class label
				String profession = profIndex[0];
				profession = profession.toLowerCase();
				labels.add(profession);
				
				// initialize a list of <string, integer> pairs
				StringIntegerList indicesSIL = new StringIntegerList();
				// and store into it each instance's <lemma, count> list
				indicesSIL.readFromString(profIndex[1]);
				
				for (StringInteger si: indicesSIL.getIndices()) {
					// add each lemma to vocabulary map and draw its index; set
					// its count for this instance at that position of the vector
					vector.set(processString(si.getString()),(double)si.getValue());
				}
				
				// create a Mahout-ready vector out of this instance's vector
				MahoutVector mahoutVector = new MahoutVector();
				mahoutVector.setClassifier(profession); 
				mahoutVector.setVector(vector);
				vectors.add(mahoutVector);
			}
			// return list of all vectors in the associated file
			return vectors;
			
		} finally {
			// tidy up by closing the buffered reader
			br.close();
		}
	}
	
	
	public void createSeqFile() throws IOException
	{
		// set the path to the index file
		String indexPath = "/user/hadoop06/output004";
		
		// set up the filesystem
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);
		Path seqFilePath = new Path(indexPath);
		// non-recursively remove any existing version of the sequence file first
		fs.delete(seqFilePath,false);
		
		// set up the writer for the sequence file
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, seqFilePath, Text.class, VectorWritable.class);

		try
		{
			// create a list of vectors using the supplied path
			List<MahoutVector> vectors = vectorize(indexPath);

			// Init the labels
			
			for (MahoutVector vector : vectors)
			{
				// write a copy of the current vector
				VectorWritable vectorWritable = new VectorWritable();
				vectorWritable.set(vector.getVector());
				
				// add the class label and vector to the sequence file
				writer.append(new Text("/" + vector.getClassifier() + "/"), vectorWritable);
			}
		}

		finally
		{
			// tidy up by closing the sequence file
			writer.close();
		}
	}
	
	
	protected int processString(String data)
	{
		// pull the index of the given string
		Integer wordIndex = vocab.get(data);
		
		// if unseen, add to vocabulary at next position
		if (wordIndex == null)
		{
			wordIndex = wordCount++;
			vocab.put(data, wordIndex);
		}
		
		// return the linear position (==index) of the vocabulary item
		return wordIndex;
	}
	
	// return the list of labels for interpretation of Mahout output vector
	public List<String> getLabelList() {
		
		List<String> labelList = new ArrayList<String>(this.labels);
		Collections.sort(labelList, String.CASE_INSENSITIVE_ORDER);
		return labelList;
	}
}
