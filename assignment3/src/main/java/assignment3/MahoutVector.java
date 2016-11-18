package assignment3;

import org.apache.mahout.math.Vector;

public class MahoutVector {
	// implementation of a Mahout-ready feature vector for a data instance
	
	// class label associated with this instance
	private String classifier;
	// Mahout vector of features
	private Vector vector;
	
	// get+set methods for this instance's features
	
	public Vector getVector() {
		return vector;
	}
	
	public void setVector(Vector vector) {
		this.vector = vector;
	}
	
	// get+set methods for this instance's class
	
	public String getClassifier() {
		return classifier;
	}
	
	public void setClassifier(String classifier) {
		this.classifier = classifier;
	}

}
