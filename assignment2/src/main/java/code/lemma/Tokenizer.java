package code.lemma;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.functors.ForClosure;

public class Tokenizer {

	public Tokenizer() {
		// TODO Auto-generated constructor stub
	}

	public List<String> tokenize(String sentence) {
		StringTokenizer st = new StringTokenizer(sentence, " \t\n\r\f,.:;?![]()'"); //Annie- use this class to do most of the work
		
		// TODO Annie
		return null;
	}
	
	public List<String> sentenceTokenize(String text){
		// TODO Annie
		return null;
	}
	
	
	
	public List<String> lemmatize(String text) {
		
		List<String> sentences = sentenceTokenize(text);
		
		List<String> tokens = new ArrayList<>();
		
		for (String sentence : sentences) {
			List<String> sentenceTokens = sentenceTokenize(sentence);
			tokens.addAll(sentenceTokens);
			
		}
		
		return null;
	}
	
	
}
