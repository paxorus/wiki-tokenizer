package code.lemma;

import java.util.ArrayList;
import java.util.List;

public class Tokenizer {
	ArrayList<String> stopWords;
	ArrayList<String> tokens;

	public Tokenizer() {

		// Manually adding elements temporarily. In final probably want to read
		// in stop words from file and add them in loop.
		stopWords = new ArrayList<String>();
		tokens = new ArrayList<String>();
		stopWords.add("a");
		stopWords.add("the");
		stopWords.add("at");
		// etc etc
	}

	public List<String> tokenize(String sentence) {
		// TODO Implement the lemmatizer and get a complete list of stop words

		String removedString = removeNonLetters(sentence);
		removedString = removedString.toLowerCase();
		String toks[] = removedString.split("\\s+");
		for (String word : toks) {
			if (!stopWords.contains(word)) {
				// tokens.add(lemma(word));
				tokens.add(word);

			}

		}

		return tokens;
	}

	public String removeNonLetters(String sentence) {
		StringBuffer s = new StringBuffer();
		for (int i = 0; i < sentence.length(); i++) {
			char c = sentence.charAt(i);
			if (Character.isLetter(c)) {
				s.append(c);
			} else if (c >= '0' && c <= '9') {
				// If number is next to letter we assume we want to keep that
				// number as part of word i.e 4chan. Requires additional i+1
				// check in order to not throw error.
				if (i + 1 < sentence.length()) {
					if (Character.isLetter(sentence.charAt(i + 1))) {
						s.append(c);
					}
				}
			} else if (c == ' ') {
				s.append(c);
			}
		}
		return s.toString();
	}

	public String lemma(String word) { // TODO implement lemmatization here

		return word;
	}

}
