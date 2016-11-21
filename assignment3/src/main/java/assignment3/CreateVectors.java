package assignment3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
		
		MahoutVector mahoutVector = new MahoutVector();
		
		if (profIndex.length == 2) {
			String profession = profIndex[0];
			profession = profession.toLowerCase();
			this.labels.add(profession);
			
			// initialize a list of <string, integer> pairs
			StringIntegerList indicesSIL = new StringIntegerList();
			// and store into it each instance's <lemma, count> list
			
			indicesSIL.readFromString(profIndex[1]);

			// initialize a new sparse vector for this line with attributes:
			// cardinality: estimate of initialized sparseness
			// initial size: size of a double hashmap representing the vector
			int listSize = indicesSIL.getIndices().size();
			Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, listSize*2);
			for (StringInteger si: indicesSIL.getIndices()) {
				// add each lemma to vocabulary map and draw its index; set
				// its count for this instance at that position of the vector
				vector.set(processString(si.getString()),(double)si.getValue());
			}
			
			// uncomment to show successful vector generation
			// System.out.println(vector);
			
			// create a Mahout-ready vector out of this instance's vector
			mahoutVector.setClassifier(profession); 
			mahoutVector.setVector(vector);
		}
		
		return mahoutVector;
		
	}	
	
	public void createSeqFile(String seqPathTrain) throws IOException
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
		
		
		// testset output
		// set up the filesystem, again
		Configuration conf3 = new Configuration();
		FileSystem fs3 = FileSystem.get(conf3);

		// read until nothing remains in the stream
		try {
			//set up test set output file
			File file = new File("testset.txt");
			file.createNewFile();
			BufferedWriter testFile = new BufferedWriter(new FileWriter(file));
			
			// index for pretty reporting and taking 10% of the data as test instances
			int seen = 0;
			
			String line = stream.readLine();
			
			// set up the writer for the sequence file
			SequenceFile.Writer writerTrain = SequenceFile.createWriter(fs, conf, seqFilePathTrain, Text.class, VectorWritable.class);
			try {
				
				// trainset the rest, without storing it in memory
				while (seen<400000) {	
					
					//every 10th instance put in the test set
					if (seen % 10 == 0) {
						
						// write this line straight back into testfile
						// TODO repair this line
						testFile.write(line);
						testFile.newLine();
						
					} else {
						// generate vector
						MahoutVector vec = processVec(line);
						// write a copy of the current vector
						if (!vec.isEmpty()) {
							VectorWritable vectorWritable = new VectorWritable();
							vectorWritable.set(vec.getVector());
							
							// add the class label and vector to the sequence file
							writerTrain.append(new Text("/" + vec.getClassifier() + "/"), vectorWritable);
						}
						
					}
					
					// update current line; end of stream returns null
					line = stream.readLine();
					
					// report
					seen++;
					if (seen % 10000 == 0) {
						System.out.println(String.valueOf(seen) + " vectors processed.  Most recent: ");
						//System.out.println(String.valueOf(vec.getClassifier())+": "+String.valueOf(vec.getVector()));
					}
				}
				
			} finally {	
				// tidy up by closing the sequence file
				writerTrain.close();
				// TODO close the testset input file
				testFile.flush();
				testFile.close();
			}
			
		} finally {
			// tidy up by closing the buffered reader
			stream.close();
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
