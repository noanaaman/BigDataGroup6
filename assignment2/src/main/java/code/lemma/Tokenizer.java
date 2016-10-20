package code.lemma;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;

public class Tokenizer {
	
	Stemmer stemmer = new Stemmer();

	public Tokenizer() {
		// TODO Auto-generated constructor stub
	}

	public List<String> tokenize(String text) {
		StringTokenizer st = new StringTokenizer(text, " \t\n\r\f,.:;?<>{}![]|-_()'"); 
		List<String> tokens = new ArrayList<>();
		
		while (st.hasMoreTokens()) {
			String token = st.nextToken();
			if (StringUtils.isAlpha(token)) {
				tokens.add(token.toLowerCase());
			}
			
		}
		// TODO Annie
		return tokens;
	}
	
	
	public List<String> getLemmas(String text) {
		
		List<String> tokens = tokenize(text);
		List<String> lemmas = new ArrayList<>();
				
		for (String token : tokens) {
			String lemma = stemmer.stem(token);
			lemmas.add(lemma);
		}
		
		return lemmas;
	}
	

	
	
	
	
}
