package assignment3;

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
			
		Path seqFilePath = new Path("???");
			
		TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		trainNaiveBayes.setConf(conf);
			
		String sequenceFile = "???";
		String outputDirectory = "???";
		String tempDirectory = "???";
			
		fs.delete(new Path(outputDirectory),true);
		fs.delete(new Path(tempDirectory),true);
			
		trainNaiveBayes.run(new String[] { "--input", sequenceFile, "--output", outputDirectory, "-el", "--overwrite", "--tempDir", tempDirectory });
		// Train the classifier
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(new Path(outputDirectory), conf);

		System.out.println("features: " + naiveBayesModel.numFeatures());
		System.out.println("labels: " + naiveBayesModel.numLabels());
	    
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
		
		CreateVectors create = new CreateVectors("pathtoindexfile"); 
		List<MahoutVector> vectors = create.vectorize();
		
	    int total = 0;
	    int success = 0;
	    
	    for (MahoutVector mahoutVector : vectors)
	    {
	    	Vector prediction = (Vector) classifier.classifyFull(mahoutVector.getVector());
	    	
	    	// They sorted alphabetically 
	    	// 0 = anomaly, 1 = normal (because 'anomaly' > 'normal') 
	    	double anomaly = (Double) prediction.get(0);
	    	double normal = (Double) prediction.get(1);
	    	
	    	String predictedClass = "anomaly";
	    	if (normal > anomaly)
	    	{
	    		predictedClass="normal";
	    	}

	    	if (predictedClass.equals(mahoutVector.getClassifier()))
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

