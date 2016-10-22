package code.lemma;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

public class Tokenizer {
	
	public Set<String> stopWords;
	public Map<String, String> lemmaRules;

	public Tokenizer() {
		loadStopWords();
		loadLemmaRules();
	}

	public List<String> process(String sentence) {
		
		// lowercase
		sentence = sentence.toLowerCase();
		
		// tokenize
		String[] tokens = tokenize(sentence);
		
		// normalization
		List<String> normalized = new ArrayList<String>();
		for (String word : tokens) {
			
			// lemmatize
			word = lemmatize(word);
			
			// remove stopwords and numbers
			if (!stopWords.contains(word) && !StringUtils.isNumeric(word)) {
				normalized.add(word);
			}
		}

		return normalized;
	}

	public String[] tokenize(String sentence) {
		StringBuffer s = new StringBuffer();
		for (int i = 0; i < sentence.length(); i++) {
			char c = sentence.charAt(i);
			if (Character.isLetterOrDigit(c)) {
				s.append(c);
			} else if (c == ' ' || c == '\t' || c == '\r') {
				s.append(' ');
			} else {
				s.append(" " + c + " ");// pad symbol with space
			}
		}
		return s.toString().split("\\s+");
	}

	public String lemmatize(String word) {
		String lemma = lemmaRules.get(word);
		return (lemma == null) ? word : lemma;
	}
	
	public void loadLemmaRules() {
		// courtesy of Michal Boleslav Mechura
		// Accessed http://www.lexiconista.com/datasets/lemmatization/ on October 15, 2016
		lemmaRules = new HashMap<String, String>();
		BufferedReader reader = fetch("lemmas.txt");
		
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\t");// [demur, demurred]
				lemmaRules.put(parts[1], parts[0]);// demurred -> demur
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void loadStopWords() {
		// courtesy of XPO6
		// Accessed http://xpo6.com/list-of-english-stop-words/ on October 15, 2016
		stopWords = new HashSet<String>();
		BufferedReader reader = fetch("stopwords.txt");
		
        String line;
        try {
			while ((line = reader.readLine()) != null) {
			  stopWords.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public BufferedReader fetch(String fileName) {
        ClassLoader cl = this.getClass().getClassLoader();
        return new BufferedReader(new InputStreamReader(cl.getResourceAsStream(fileName)));
	}

}
