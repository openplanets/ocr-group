package opfscape.hackathon.ocrquality.dictionary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class HBaseDictionaryManager {

	private static final Logger LOGGER = Logger
			.getLogger(HBaseDictionaryManager.class);

	private static final String CENTURY = "century";

	private final HTable table;
	private final HBaseAdmin admin;

	public HBaseDictionaryManager(final String language, final boolean create)
			throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		admin = new HBaseAdmin(hbaseConfig);

		if (admin.tableExists(language)) {
			LOGGER.info(String.format(
					"Dictionary table for '%s' language exists.", language));
			if (create) {
				LOGGER.info(String.format("Dropping table for '%s' language.",
						language));
				admin.disableTable(language);
				admin.deleteTable(language);
			}
		}
		if (create) {
			LOGGER.info(String.format("Creating table for '%s' language.",
					language));
			HTableDescriptor tableDesc = new HTableDescriptor(language);
			tableDesc.addFamily(new HColumnDescriptor(CENTURY));
			admin.createTable(tableDesc);
			LOGGER.info(String.format(
					"Successfully created table for '%s' language.", language));
		}

		table = new HTable(hbaseConfig, language);
		admin.close();
	}

	public boolean addWordFrequency(String word, int century, int frequency) {
		try {
			Put record = new Put(Bytes.toBytes(word));
			record.add(Bytes.toBytes(CENTURY), Bytes.toBytes(century),
					Bytes.toBytes(frequency));
			table.put(record);
			return true;
		} catch (Exception e) {
			LOGGER.error("Couldn't insert word: " + word);
			LOGGER.error(e.getMessage(), e);
			return false;
		}
	}

	public int getWordFrequency(String word, int century) {
		try {
			Get g = new Get(Bytes.toBytes(word));
			Result r = table.get(g);
			byte[] value = r.getValue(Bytes.toBytes(CENTURY),
					Bytes.toBytes(century));
			if (value != null) {
				return Bytes.toInt(value);
			} else {
				return 0;
			}
		} catch (Exception e) {
			LOGGER.error("Couldn't retrieve word: " + word);
			LOGGER.error(e.getMessage(), e);
			return -1;
		}
	}

}
