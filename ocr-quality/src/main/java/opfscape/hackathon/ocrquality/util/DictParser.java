package opfscape.hackathon.ocrquality.util;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Scanner;
import java.util.TreeSet;

public class DictParser {

	@SuppressWarnings("unused")
	private final static String enDictName = "dict_en-de.txt";
	private final static String deDictName = "dict_de-en.txt";
	
	/**
	 * Read a dict.cc dictionary file, extract terms from source language and
	 * return tokens as set
	 * 
	 * @param fileName
	 * @return
	 */
	private static TreeSet<String> readDictFile(String fileName) {
		TreeSet<String> set = new TreeSet<String>();
		InputStream is = DictParser.class.getClassLoader().getResourceAsStream(
				fileName);

		Scanner scanner = new Scanner(is);
		String line;
		while (scanner.hasNextLine()) {
			line = scanner.nextLine();

			// Omit comments and empty lines
			if (line.trim().startsWith("#") || line.trim().equals("")) {
				continue;
			}

			// split on tab
			final String[] lineSplits = line.split("\t");

			if (lineSplits.length < 2) {
				continue;
			}

			// take the portion before the first tab (term in source language)
			// and remove remarks in curly/square brackets
			final String cleanContent = lineSplits[0].replaceAll(
					"(\\{[a-zA-Z.,;]*\\})|(\\[.*\\])", "").trim();

			// build tokens and add them to the set
			final String[] cleanSplits = cleanContent.split("\\s");
			for (String s : cleanSplits) {
				final String token = s.trim().replaceAll("\\<|\\>|\\(|\\)|,|;",
						"");
				// omit single-char tokens
				if (token.length() > 1)
					set.add(token.toLowerCase());
			}
		}
		scanner.close();
		return set;
	}


	public static void main(String[] args) {
		//create sorted set with terms from dict.cc file
		TreeSet<String> set = readDictFile(deDictName);
		
		//write the terms to a new file
		try {
//			final String path = "src/main/resources/20jh_lex.txt";
			writeLexFile(set, "20jh_lex.txt");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}	
	}

	private static void writeLexFile(TreeSet<String> set, String filePath) throws IOException {
		File outFile = new File(filePath);
		
		if(outFile.exists() && outFile.isDirectory()){
			throw new IOException("File " + filePath + " is a directory.");
		}
		if(outFile.exists() && !outFile.canWrite()){
			throw new IOException("File " + filePath + " is not writable.");
		}
		
		FileOutputStream fos = new FileOutputStream(outFile);
		OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
		
		//mimics the format of an INL lexicon from the Impact project and thus has a (fake) word frequency in each line 
		for (String s : set) {
			osw.write("     1 " + s + "\n");
		}
		
		osw.close();
	}
}
