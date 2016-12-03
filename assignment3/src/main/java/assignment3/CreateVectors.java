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



/**
 * This class is used to split the dataset into train and test sets, and create a sequenceFile for Mahout's classifier.
 *
 * Every tenth line is copied to the test set, the rest are written to the sequence file.
 * 
 * Based mostly on the example posted on the course forum.
 */
public class CreateVectors {
	
	private static Integer wordCount = 1;
	private static final Map<String, Integer> vocab = Maps.newHashMap();
	private String indexPath; 
	private HashSet<String> labels;
	
	public CreateVectors(String indexPath)
	{
		this.indexPath = indexPath;
		this.labels = new HashSet<String>();
	}
	
	/**
	 * Turn one line in the index into a Mahout vector
	 * 
	 */
	public MahoutVector processVec(String line) throws IOException {
		
		// split the line on tabs into profession and features
		String[] profIndex = line.split("\t");
		MahoutVector mahoutVector = new MahoutVector();
		
		//if line is in the right format the array will have 2 elements
		if (profIndex.length == 2) {
			String profession = profIndex[0];
			profession = profession.toLowerCase();
			this.labels.add(profession);
			
			// create a list of <string, integer> pairs
			StringIntegerList indicesSIL = new StringIntegerList();	
			indicesSIL.readFromString(profIndex[1]);

			// initialize a new sparse vector for this line with attributes:
			// cardinality: estimate of initialized sparseness
			// initial size: size of a double hashmap representing the vector
			int listSize = indicesSIL.getIndices().size();
			Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, listSize*2);
			
			for (StringInteger si: indicesSIL.getIndices()) {
				//add pairs of <lemma index,frequency> to the feature vector
				vector.set(processString(si.getString()),(double)si.getValue());
			}
			
			
			// create a Mahout-ready vector out of this instance's vector
			mahoutVector.setClassifier(profession); 
			mahoutVector.setVector(vector);
		}
		
		return mahoutVector;
		
	}	
	
	
	/**
	 * Go through the entire input file, write the vectors to either the test or train set.
	 *
	 */
	public void createSeqFile(String seqPathTrain) throws IOException
	{
		String indexPath = this.indexPath;
		
		// set up the filesystem and remove old files - for the train set
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path seqFilePathTrain = new Path(seqPathTrain);
		fs.delete(seqFilePathTrain,false);
		
		// read input file
		Configuration conf2 = new Configuration();
		FileSystem fs2 = FileSystem.get(conf2);
		FSDataInputStream stream = fs2.open(new Path(indexPath));
		
		
		// testset output
		Configuration conf3 = new Configuration();
		FileSystem fs3 = FileSystem.get(conf3);

		try {
			File file = new File("testset.txt");
			file.createNewFile();
			BufferedWriter testFile = new BufferedWriter(new FileWriter(file));
			
			int seen = 0;
			
			String line = stream.readLine();
			
			// set up the writer for the sequence file
			SequenceFile.Writer writerTrain = SequenceFile.createWriter(fs, conf, seqFilePathTrain, Text.class, VectorWritable.class);
			try {
				
				// limit size of data set to avoid memory issues
				while (seen<600000) {	
					
					//every 10th instance put in the test set
					if (seen % 10 == 0) {
						
						testFile.write(line);
						testFile.newLine();
						
					} else {
						
						//create vector from line string
						MahoutVector vec = processVec(line);
						if (!vec.isEmpty()) {
							VectorWritable vectorWritable = new VectorWritable();
							vectorWritable.set(vec.getVector());
							
							// add the class label and vector to the sequence file
							writerTrain.append(new Text("/" + vec.getClassifier() + "/"), vectorWritable);
						}
						
					}
					
					line = stream.readLine();
					
					seen++;
					
				}
				
			} finally {	

				// tidy up by closing the sequence file

				writerTrain.close();
				testFile.flush();
				testFile.close();
			}
			
		} finally {
			stream.close();
		}
	}
	
	
	protected int processString(String data)
	{
		Integer wordIndex = vocab.get(data);
		
		// if unseen, add to vocabulary at next position
		if (wordIndex == null)
		{
			wordIndex = wordCount++;
			vocab.put(data, wordIndex);
		}
		
		// return the index mapping of the vocabulary item
		return wordIndex;
	}
	
	// return the sorted list of labels for interpretation of Mahout output vector
	public List<String> getLabelList() {
		
		List<String> labelList = new ArrayList<String>(this.labels);
		Collections.sort(labelList, String.CASE_INSENSITIVE_ORDER);
		return labelList;
	}
}
