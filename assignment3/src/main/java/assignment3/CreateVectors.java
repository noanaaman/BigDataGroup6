package assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
	
	// processes one line into vector
	public MahoutVector processVec(String line) throws IOException {
		
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

		// initialize a new sparse vector for this line with attributes:
		// cardinality: estimate of initialized sparseness
		// initial size: size of a double hashmap representing the vector
		int listSize = indicesSIL.getIndices().size();
		Vector vector = new RandomAccessSparseVector(vocab.size()*2+listSize*2, listSize*2);
		for (StringInteger si: indicesSIL.getIndices()) {
			// add each lemma to vocabulary map and draw its index; set
			// its count for this instance at that position of the vector
			vector.set(processString(si.getString()),(double)si.getValue());
		}
		System.out.println(vector);
		
		// create a Mahout-ready vector out of this instance's vector
		MahoutVector mahoutVector = new MahoutVector();
		mahoutVector.setClassifier(profession); 
		mahoutVector.setVector(vector);
		
		return mahoutVector;
		
	}
	
	
	public List<MahoutVector> createSeqFile(String seqPathTrain) throws IOException
	{
		// set the path to the index file
		String indexPath = this.indexPath;
		
		// set up the filesystem
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//Path seqFilePath = new Path(indexPath);
		Path seqFilePathTrain = new Path(seqPathTrain);
		// non-recursively remove any existing version of the sequence file first
		fs.delete(seqFilePathTrain,false);
		
		// set up the filesystem, again
		Configuration conf2 = new Configuration();
		FileSystem fs2 = FileSystem.get(conf2);
		// set up datastream
		FSDataInputStream stream = fs2.open(new Path(indexPath));
		// testset size
		int testSize = 6080;
		// testset
		List<MahoutVector> testVectors = Lists.newArrayList();
		
		// read until nothing remains in the stream
		try {
			String line = stream.readLine();
			// testset the first chunk
			for (int i=0; i<testSize; i++){
				
				// generate mahout vector
				MahoutVector vec = processVec(line);
				// add to testset
				testVectors.add(vec);
				// update line
				line = stream.readLine();
				
			}
			// set up the writer for the sequence file
			SequenceFile.Writer writerTrain = SequenceFile.createWriter(fs, conf, seqFilePathTrain, Text.class, VectorWritable.class);
			try {
				
				// trainset the rest, without storing it in memory
				while (line != null) {				
					
					// generate vector
					MahoutVector vec = processVec(line);
					// write a copy of the current vector
					VectorWritable vectorWritable = new VectorWritable();
					vectorWritable.set(vec.getVector());
					
					// add the class label and vector to the sequence file
					writerTrain.append(new Text("/" + vec.getClassifier() + "/"), vectorWritable);
					
					// update current line; end of stream returns null
					line = stream.readLine();
				}
				
			} finally {	
				// tidy up by closing the sequence file
				writerTrain.close();
			}
			
		} finally {
			// tidy up by closing the buffered reader
			stream.close();
		}
		
		// return the testset vectors
		return testVectors;
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
