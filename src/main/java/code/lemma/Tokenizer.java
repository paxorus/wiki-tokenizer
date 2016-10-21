
import java.util.ArrayList;
import java.util.List;


public class Tokenizer {
	ArrayList<String> stopWords;

	public Tokenizer() {
		// TODO Auto-generated constructor stub
		
	}

	public List<String> tokenize(String sentence) {
		// TODO implement your tokenizing code here
		
		String removedString = removeNonLetters(sentence);
		
		System.out.println(removedString);
		return null;
	}
	public String removeNonLetters(String sentence){
		StringBuffer s = new StringBuffer();
		for (int i=0 ; i < sentence.length(); i++) {
			char c= sentence.charAt(i);
	        if(Character.isLetter(c)) {
	        	s.append(c);
	        	}
	        else if(c >= '0' && c <= '9'){
	        	//If number is next to letter we assume we want to keep that number as part of word i.e 4chan
	        	if(Character.isLetter(sentence.charAt(i+1))){
	        		s.append(c);
	        	}
	        }
	        else if(c==' '){
	        	s.append(c);
	        }
	        else{
	        }
		}
	   return s.toString();
		
	
	}
		
}
