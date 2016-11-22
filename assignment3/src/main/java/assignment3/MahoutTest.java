package assignment3;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;
//import org.apache.mahout.classifier.df.tools.Describe;


public class MahoutTest {
	
	public static void trainNB() throws Throwable
	{
		// begin by setting up Mahout job background details
		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "hadoop06");
		FileSystem fs = FileSystem.get(conf);
			
		// set up NB
		TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		trainNaiveBayes.setConf(conf);
			
		// set up paths
		String sequenceFileTrain = "/user/hadoop06/trainseqfilepath";
		String sequenceFileTest = "/user/hadoop06/testseqfilepath";
		String outputDirectory = "/user/hadoop06/output005";
		String tempDirectory = "/user/hadoop06/temp";
		String indexPath = "/user/hadoop06/output004/part-r-00000";
		String filteredIndex = "/user/hadoop06/filteredIndex";
	    String testIndexPath = "/user/hadoop06/testset.txt";
			
		// clear out current versions of directories recursively if they exist
		fs.delete(new Path(outputDirectory),true);
		fs.delete(new Path(tempDirectory),true);
		

		// filter out features to attempt to avoid OOM errors
		
		FilterFeatures filter = new FilterFeatures(indexPath,filteredIndex);
		filter.countFeatures();
		filter.removeFeatures();

		// create the train and test files, collect the labels 
		CreateVectors create = new CreateVectors(filteredIndex); 
		create.createSeqFile(sequenceFileTrain);
		List<String> professionsList = create.getLabelList();

		
		// Train the classifier
		File sFT = new File(sequenceFileTrain);
		String sftPath = sFT.getAbsolutePath();
		//trainNaiveBayes.run(new String[] { "--input", sftPath, "--output", outputDirectory, "--overwrite", "--tempDir", tempDirectory });
		ToolRunner.run(conf, trainNaiveBayes, new String[] { "--input", sftPath, "--output", outputDirectory, "--overwrite", "--tempDir", tempDirectory });
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(new Path(outputDirectory), conf);
		
		// close this fs
		//fs.close();

		
		System.out.println("features: " + naiveBayesModel.numFeatures());
		System.out.println("labels: " + naiveBayesModel.numLabels());
	    
		// Use the model to create a classifier for new data
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
		
	    int total = 0;
	    int success = 0;
	    
		// read from the test index
		Configuration confTest = new Configuration();
		FileSystem fsTest = FileSystem.get(confTest);
		FSDataInputStream stream = fsTest.open(new Path(testIndexPath));
		
		try {
						
			String line = stream.readLine();
			
			while (line != null) {
				
				// generate vector
				MahoutVector mahoutVector = create.processVec(line);
				
				Vector prediction = classifier.classifyFull(mahoutVector.getVector());
		    	    	  
	            
	            //indexes of the top 3 
	            //get the index of the max element, then set the max element to 0 (in the copy)
	            //then get the the new max element's index, repeat for #3

	            int indexofTop1 = prediction.maxValueIndex();
	            prediction.set(indexofTop1, 0);
	            int indexofTop2 = prediction.maxValueIndex();
	            prediction.set(indexofTop2, 0);
	            int indexofTop3 = prediction.maxValueIndex();
	            
	            //get the top three predictions
	            String prediction1 = professionsList.get(indexofTop1);
	            String prediction2 = professionsList.get(indexofTop2);
	            String prediction3 = professionsList.get(indexofTop3);
	            
	            // if one of the top three guesses is correct, consider it success
		    	if (prediction1.equals(mahoutVector.getClassifier())
	            || prediction2.equals(mahoutVector.getClassifier())
	            || prediction3.equals(mahoutVector.getClassifier()))
		    	{ 
		    		success++;
		    	}
		    	total ++;
		    	
		    	// reporting
		    	if (total%2000==0){
		    		System.out.println(String.valueOf(total)+" vectors tested so far");
		    	}
		    	
				line = stream.readLine();
			}
			
		} finally {
			// close the testset stream
			stream.close();
			fsTest.close();
		}
	    
	    System.out.println(total + " : " + success + " : " + (total - success) + " " + ((double)success/total));
	}
	
	public static void testNB() throws Throwable {
		
	}
	
	

	public static void main(String[] args) throws Throwable
	{
		// runs Naive Bayes test
		trainNB();

	}
}

