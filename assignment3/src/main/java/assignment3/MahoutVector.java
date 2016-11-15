package assignment3;

import org.apache.mahout.math.Vector;

public class MahoutVector {
	
	private String classifier;
	private Vector vector;
	
	
	public Vector getVector() {
		return vector;
	}
	public void setVector(Vector vector) {
		this.vector = vector;
	}
	public String getClassifier() {
		return classifier;
	}
	public void setClassifier(String classifier) {
		this.classifier = classifier;
	}

}
