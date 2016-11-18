package assignment3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;

public class MahoutTest {

	public static void train() throws Throwable
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);
			
		Path seqFilePath = new Path("/user/hadoop06/seqfilepath");
			
		TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		trainNaiveBayes.setConf(conf);
			
		String sequenceFile = "seqfile";
		String outputDirectory = "/user/hadoop06/output005";
		String tempDirectory = "/user/hadoop06/temp";
			
		fs.delete(new Path(outputDirectory),true);
		fs.delete(new Path(tempDirectory),true);
			
		trainNaiveBayes.run(new String[] { "--input", sequenceFile, "--output", outputDirectory, "-el", "--overwrite", "--tempDir", tempDirectory });
		// Train the classifier
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(new Path(outputDirectory), conf);

		System.out.println("features: " + naiveBayesModel.numFeatures());
		System.out.println("labels: " + naiveBayesModel.numLabels());
	    
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
		
		CreateVectors create = new CreateVectors("/user/hadoop06/output004"); 
		List<MahoutVector> vectors = create.vectorize();
		
		List<String> professionsList = create.getLabelList();
		
	    int total = 0;
	    int success = 0;
	    
	    for (MahoutVector mahoutVector : vectors)
	    {
	    	Vector<Double> prediction = (Vector<Double>) classifier.classifyFull(mahoutVector.getVector());
	    	
	    	// Professions are sorted alphabetically ?? hopefully
	    	Vector<Double> predictionCopy = (Vector<Double>) prediction.clone();
	    	Comparator<Double> c = new Comparator<Double>(){
                public int compare(Double s1,Double s2){
                	return s1.compareTo(s2);
              }};
            predictionCopy.sort(c);
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
		train();
	}
}

