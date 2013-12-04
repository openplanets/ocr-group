package opfscape.hackathon.ocrquality.solr;

import opfscape.hackathon.ocrquality.util.SolrUtil;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

public class SolrSearcher {

	private static HttpSolrServer server = null;

	public static void main(String[] args) {	
		try {
			System.out.println(isWordInDict("test", "20"));
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}

	public static boolean isWordInDict(String word, String century)
			throws SolrServerException {
		SolrDocumentList docs = querySolr("termString:" + word + " AND century:" + century);
		
//		System.out.println("Result size = " + docs.size());
		
		return !docs.isEmpty();
	}
	
	public static boolean isWordInAnyDict(String word)
			throws SolrServerException {
		SolrDocumentList docs = querySolr("termString:" + word);
		
//		System.out.println("Result size = " + docs.size());
		
		return !docs.isEmpty();
	}
	
	public static SolrDocumentList querySolr(String queryString) throws SolrServerException{
		HttpSolrServer server = getConn();
		SolrQuery query = new SolrQuery();
		query.setQuery(queryString);
		QueryResponse rsp = server.query(query);
		return rsp.getResults();
	}

	public static HttpSolrServer getConn() {
		if (server == null) {
			server = new HttpSolrServer(SolrUtil.SOLR_URL);
		}
		return server;
	}
}
