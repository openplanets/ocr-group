package opfscape.hackathon.ocrquality.solr;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

import opfscape.hackathon.ocrquality.util.SolrUtil;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexer {

	private final static String lex16 = "16jh_clean.frq";
	private final static String lex17 = "17jh_clean.frq";
	private final static String lex18 = "18jh_clean.frq";
	private final static String lex19 = "19jh_qclean.frq";

	/**
	 * Read a Impact lexicon and return terms with frequency as HashMap
	 * 
	 * @param fileName
	 * @return
	 */
	private static TreeMap<String, Integer> readLexiconFile(String fileName) {
		TreeMap<String, Integer> result = new TreeMap<String, Integer>();
		InputStream is = SolrSearcher.class.getClassLoader().getResourceAsStream(
				fileName);
		Scanner scanner = new Scanner(is);
		String line;
		Integer freq;
		while (scanner.hasNextLine()) {
			line = scanner.nextLine().trim();
			final String[] lineSplits = line.split(" ");
			if (lineSplits.length != 2) {
				System.out.println("Strange line " + line);
			} else {
				freq = Integer.parseInt(lineSplits[0]);
				result.put(lineSplits[1], freq);
				// System.out.println();
			}
		}
		scanner.close();
		return result;
	}

	public static void main(String[] args) {

		TreeMap<String, Integer> map;
		//
		// // 16th
		// map = readLexiconFile(lex16);
		// for (Entry<String, Integer> e : map.entrySet()) {
		// System.out.println(e.getKey() + " - " + e.getValue());
		// }
		//
		// // 17th
		// map = readLexiconFile(lex17);
		// for (Entry<String, Integer> e : map.entrySet()) {
		// System.out.println(e.getKey() + " - " + e.getValue());
		// }
		//
		// // 18th
		// map = readLexiconFile(lex18);
		// for (Entry<String, Integer> e : map.entrySet()) {
		// System.out.println(e.getKey() + " - " + e.getValue());
		// }
		//
		// // 19th century
		// map = readLexiconFile(lex19);
		// for (Entry<String, Integer> e : map.entrySet()) {
		// System.out.println(e.getKey() + " - " + e.getValue());
		// }

		// modern lexicon. Not good yet
		// TreeSet<String> set = readDictFile(deDictName);
		// try {
		// writeLexFile(set,
		// "/home/philip/workspace/hadooptest/src/main/resources/20jh_lex.txt");
		// } catch (IOException e1) {
		// e1.printStackTrace();
		// }

//		map = readLexiconFile("20jh_lex.txt");
//
//		try {
//			addLexToIndex(20, map);
//		} catch (SolrServerException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	/*
	 * <field name="id" type="string" indexed="true" stored="true"
	 * required="true" multiValued="false" />
	 * 
	 * <field name="text" type="text_general" indexed="true" stored="true"/>
	 * <field name="termString" type="string" indexed="true" stored="true"/>
	 * <field name="freq" type="int" indexed="false" stored="true"/>
	 */
	private static void addLexToIndex(int century, Map<String, Integer> dict)
			throws SolrServerException, IOException {
		HttpSolrServer server =  new HttpSolrServer(SolrUtil.SOLR_URL);
		Collection<SolrInputDocument> coll = new ArrayList<SolrInputDocument>(51);
		int i = 0;

		for (Entry<String, Integer> e : dict.entrySet()) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", ++i);
			doc.addField("text", e.getKey());
			doc.addField("text_rev", e.getKey());
			doc.addField("termString", e.getKey());
			doc.addField("century", century);
			doc.addField("freq", e.getValue());
			coll.add(doc);

			if (i % 500 == 0) {
				server.add(coll);
				coll = new ArrayList<SolrInputDocument>(51);
				if (i % 2000 == 0) {
					server.commit();
				}
			}
		}
		server.add(coll);
		server.commit();
		server.optimize();
		server.shutdown();
	}

}
