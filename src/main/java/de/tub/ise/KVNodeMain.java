package de.tub.ise;

import java.io.IOException;
import java.util.Scanner;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

/**
 * Class with the main method to start the node
 */
public class KVNodeMain {

	static Logger logger = Logger.getLogger(KVNodeMain.class.getName());

	public static Configuration config;

	/**
	 * Parses arguments from command line and the configuration specified in the
	 * configuration file.
	 * Starts Receiver, i.e. the server to listen to requests
	 */
	public static void main(String[] args)
			throws ParseException, ParserConfigurationException, SAXException, IOException, InterruptedException {
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("q", "configURI", true, "URI to download the configuration file");
		options.addOption("n", "thisNode", true, "the name of this node");

		CommandLine line = parser.parse(options, args);

		// validate that all required options have been set
		if (!line.hasOption("q") || !line.hasOption("n")) {
			logger.error("Please make sure to set all options!");
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("kv", options);
			System.exit(1);
		}

		logger.info("configURI: " + line.getOptionValue("q"));
		logger.info("thisNode: " + line.getOptionValue("n"));

		KVNodeMain.config = new Configuration(line.getOptionValue("n"), line.getOptionValue("q"));
		KVNodeMain.config.parseConfig();

		//Test configuration file for valid quorum sizes
		if (!KVNodeMain.config.testQuorumSizes()){
			logger.error("Please enter valid quorum sizes. Must be equal or smaller than the number of nodes");
			System.exit(1);
		}

		//Start server to listen to requests
		try {
			Receiver receiver = new Receiver();
			receiver.start();
			receiver.blockUntilShutdown();
		} catch (Exception e) {
			logger.error("Port " + KVNodeMain.config.getReceivePort() + "  in configuration file was already taken");
			System.exit(1);
		}

		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		scanner.close();
	}
	

}


