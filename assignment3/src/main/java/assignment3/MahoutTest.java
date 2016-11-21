package assignment3;

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
		// compressing
		//conf.set("mapred.compress.map.output", "true");
		//conf.set("mapred.output.compression.type", "BLOCK"); 
		//conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
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
		
		/* 
		 * // TODO remove this comment to regenerate files
		 * 
		 */
		// filter out features to attempt to avoid OOM errors
		FilterFeatures filter = new FilterFeatures(indexPath,filteredIndex);
		filter.countFeatures();
		filter.removeFeatures();
		/*
		*
		*/
		/*
		 * // TODO remove this comment to regenerate files
		 * 
		 */
		CreateVectors create = new CreateVectors(filteredIndex); 
		// create sequence files and split out testset
		create.createSeqFile(sequenceFileTrain);
		// get labels associated with vectors
		List<String> professionsList = create.getLabelList();
		System.out.println(professionsList);	
		/*
		*
		*/
		
		// Train the classifier
		// removed "-el" before overwrite
		trainNaiveBayes.run(new String[] { "--input", sequenceFileTrain, "--output", outputDirectory, "--overwrite", "--tempDir", tempDirectory });
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(new Path(outputDirectory), conf);
		
		// close this fs
		fs.close();

		// Report!
		System.out.println("features: " + naiveBayesModel.numFeatures());
		System.out.println("labels: " + naiveBayesModel.numLabels());
	    
		// Use the model to create a classifier for new data
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
		
	    int total = 0;
	    int success = 0;
	    
		// set up the filesystem, again
		Configuration confTest = new Configuration();
		FileSystem fsTest = FileSystem.get(confTest);
		// set up datastream
		FSDataInputStream stream = fsTest.open(new Path(testIndexPath));
		
		try {
			
			// dummy CreateVectors object for line processing
			CreateVectors create2 = new CreateVectors("");
			
			String line = stream.readLine();
			
			while (line != null) {
				
				// generate vector
				MahoutVector mahoutVector = create2.processVec(line);
				
				Vector prediction = classifier.classifyFull(mahoutVector.getVector());
		    	
				/*
				 * 
				 * 
		    	// Professions are returned in alphanumeric sort;
		    	// make a copy to match up with this
		    	Vector predictionCopy = prediction.clone();	            
	            
	            //indexes of the top 3 
	            //get the index of the max element, then set the max element to 0 (in the copy)
	            //then get the the new max element's index, repeat for #3
	            int indexofTop1 = predictionCopy.maxValueIndex();
	            predictionCopy.set(indexofTop1, 0);
	            int indexofTop2 = predictionCopy.maxValueIndex();
	            predictionCopy.set(indexofTop2, 0);
	            int indexofTop3 = predictionCopy.maxValueIndex();
	            *
	            *
	            */
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
		// run further classifiers
		// trainRF();
		// trainNN();
	}
}

