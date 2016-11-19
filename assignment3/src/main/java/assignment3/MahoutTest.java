package assignment3;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
//import org.apache.mahout.classifier.df.tools.Describe;


public class MahoutTest {
	
	public static void trainNB() throws Throwable
	{
		// begin by setting up Mahout job background details
		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "hadoop06");
		FileSystem fs = FileSystem.getLocal(conf);
			
		// set up NB
		TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		trainNaiveBayes.setConf(conf);
			
		String sequenceFile = "/user/hadoop06/seqfilepath";
		String outputDirectory = "/user/hadoop06/output005";
		String tempDirectory = "/user/hadoop06/temp";
		String indexPath = "/user/hadoop06/output004/part-r-00000";
		
		// make sure we have split into trainset and testset
			
		// clear out current versions of directories recursively if they exist
		fs.delete(new Path(outputDirectory),true);
		fs.delete(new Path(tempDirectory),true);
		
		CreateVectors create = new CreateVectors(indexPath); 
		// generate vectors from testset
		List<MahoutVector> vectors = create.vectorize();
		// get labels associated with vectors
		List<String> professionsList = create.getLabelList();
		// create sequence file
		create.createSeqFile(sequenceFile);
			
		// Train the classifier
		// removed "-el" before overwrite
		trainNaiveBayes.run(new String[] { "--input", sequenceFile, "--output", outputDirectory, "--overwrite", "--tempDir", tempDirectory });
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(new Path(outputDirectory), conf);

		// Report!
		System.out.println("features: " + naiveBayesModel.numFeatures());
		System.out.println("labels: " + naiveBayesModel.numLabels());
	    
		// Use the model to create a classifier for new data
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
		
	    int total = 0;
	    int success = 0;
	    
	    for (MahoutVector mahoutVector : vectors)
	    {
	    	Vector<Double> prediction = (Vector<Double>) classifier.classifyFull(mahoutVector.getVector());
	    	
	    	// Professions are returned in alphanumeric sort;
	    	// make a copy to match up with this
	    	Vector<Double> predictionCopy = (Vector<Double>) prediction.clone();
	    	Comparator<Double> c = new Comparator<Double>(){
                public int compare(Double s1,Double s2){
                	return s1.compareTo(s2);
              }};
            Collections.sort(predictionCopy, c);
            
            //top 3 values from the prediction vector
            Double top1 = predictionCopy.get(0);
            Double top2 = predictionCopy.get(1);
            Double top3 = predictionCopy.get(2);
            
            //indexes of the top 3 
            int indexofTop1 = prediction.indexOf(top1);
            int indexofTop2 = prediction.indexOf(top2);
            int indexofTop3 = prediction.indexOf(top3);
            
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
	    }
	    System.out.println(total + " : " + success + " : " + (total - success) + " " + ((double)success/total));
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

