package code.lemma;

import java.util.List;
import java.util.ArrayList;

public class TokenizerMain {

	public static void main(String[] args) {
		List<String> articles = getArticles();
        Tokenizer t = new Tokenizer();


		System.out.println(t.tokenize(articles.get(0)));
	}

	public static List<String> getArticles() {
		List<String> articles = new ArrayList<String>();
        ClassLoader cl = this.getClass().getClassLoader();
        BufferedReader reader = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("articles.txt")));

        // iterate through file, add names
        String line;
        while ((line = reader.readLine()) != null) {
      	  articles.add(qualifyName(line));
        }
        return articles;
	}
}
