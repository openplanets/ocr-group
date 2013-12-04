package opfscape.hackathon.ocrquality.dictionary;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class HBaseDictionaryIngester {

	private static final Logger LOGGER = Logger
			.getLogger(HBaseDictionaryIngester.class);

	private static final String LANGUAGE_OPTION = "language";

	private static final String INPUT_FOLDER_OPTION = "inputfolder";

	private static final String CREATE_OPTION = "create";
	private static final String APPEND_OPTION = "append";

	private final HBaseDictionaryManager dictManager;

	public HBaseDictionaryIngester(String language, boolean create)
			throws Exception {
		dictManager = new HBaseDictionaryManager(language, create);
	}

	private void ingestFile(File inputFile) throws IOException {
		int century = 0;
		try {
			century = Integer.parseInt(inputFile.getName().substring(0, 2));
		} catch (Exception e) {
			LOGGER.error(String.format("Filename '%s' doesn't begin from a century's number", inputFile.getName()));
			LOGGER.error(String.format("skipping file: '%s'", inputFile.getName()));
			return;
		}
		
		InputStream is = new FileInputStream(inputFile);
		Scanner scanner = new Scanner(is);
		String line;
		Integer frequency;
		while (scanner.hasNextLine()) {
			line = scanner.nextLine().trim();
			String[] lineSplits = line.split(" ");
			if (lineSplits.length != 2) {
				LOGGER.warn(String.format("Strange line: %s", line));
			} else {
				frequency = Integer.parseInt(lineSplits[0]);
				dictManager.addWordFrequency(lineSplits[1], century, frequency);
			}
		}
		scanner.close();
		is.close();
	}

	private void ingestFolder(File inputFolder) throws IOException {
		for (int i = 0; i < inputFolder.listFiles().length; i++) {
			File inputFile = inputFolder.listFiles()[i];
			LOGGER.info(String.format("processing file: '%s'",
					inputFile.getName()));
			ingestFile(inputFile);
		}
	}

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		
		options.addOption(OptionBuilder.withArgName("language").hasArg()
				.withDescription("dictionary language").create(LANGUAGE_OPTION));
		
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("dictionary files location")
				.create(INPUT_FOLDER_OPTION));
			
		options.addOption("create", false, "create new dictionary table");
		options.addOption("append", false,
				"append to existing dictionary table");

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException e) {
			LOGGER.error("Error parsing command line.", e);
			System.exit(-1);
		}

		if (!cmdline.hasOption(LANGUAGE_OPTION)
				|| !cmdline.hasOption(INPUT_FOLDER_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(
					HBaseDictionaryIngester.class.getCanonicalName(), options);
			System.exit(-1);
		}

		if (!cmdline.hasOption(CREATE_OPTION)
				&& !cmdline.hasOption(APPEND_OPTION)) {
			LOGGER.error(String.format("Must specify either -%s or -%s",
					CREATE_OPTION, APPEND_OPTION));
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(
					HBaseDictionaryIngester.class.getCanonicalName(), options);
			System.exit(-1);
		}

		String path = cmdline.getOptionValue(INPUT_FOLDER_OPTION);
		File inputFolder = new File(path);
		String language = cmdline.getOptionValue(LANGUAGE_OPTION);
		boolean create = cmdline.hasOption(CREATE_OPTION);

		HBaseDictionaryIngester ingester = new HBaseDictionaryIngester(
				language, create);
		ingester.ingestFolder(inputFolder);
	}

}
