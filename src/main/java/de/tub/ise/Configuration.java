package de.tub.ise;

import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

/**
 * Class for Key Value Store configuration You shouldn't have to change anything
 * here
 */
public class Configuration {

    private String myNode;
    private String configURI;
    private HashMap<String, String> hosts = new HashMap<String, String>(); // node -> ip:port
    private int receivePort;
    private int qsize;
    private int qreadsize;
    private int qwritesize;

    static Logger logger = Logger.getLogger(Configuration.class.getName());


    public Configuration(String myNode, String configURI) {
        this.myNode = myNode;
        this.configURI = configURI;
    }

    public void parseConfig() throws ParserConfigurationException, SAXException, IOException {

        // Parse config xml file
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        Document doc = docBuilder.parse(configURI);
        doc.getDocumentElement().normalize();

        NodeList nodeList = doc.getElementsByTagName("host");

        // Add nodes to node list hashmap
        // node -> ip:port
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node n = nodeList.item(i);
            try {
                Integer.parseInt(n.getAttributes().getNamedItem("ip").getNodeValue().split(":")[1]);
                hosts.put(n.getAttributes().getNamedItem("node").getNodeValue(),
                        n.getAttributes().getNamedItem("ip").getNodeValue());
            } catch (NumberFormatException nfe) {
                logger.error("Port of " + n.getAttributes().getNamedItem("node").getNodeValue() + " not int");
                System.exit(1);
            }
        }

        // Set listening port for this node
        try {
            receivePort = Integer.parseInt(hosts.get(myNode).split(":")[1]);
            logger.info("thisNode - host: " + hosts.get(myNode).split(":")[0] + " port: " + receivePort);
        } catch (NumberFormatException nfe) {
            logger.error("Receive Port not int");
            System.exit(1);
        }

        // Set N: quorum size = number of nodes
        qsize = hosts.size();

        // For Quorum (N,R, W), get values for R and W
        try {
            qreadsize = Integer.parseInt(doc.getDocumentElement().getAttributes().getNamedItem("qread").getNodeValue());
            qwritesize = Integer
                    .parseInt(doc.getDocumentElement().getAttributes().getNamedItem("qwrite").getNodeValue());
        } catch (NumberFormatException nfe) {
            logger.error("Quorum sizes not int");
            System.exit(1);
        }
    }

    /**
     * Check for valid quorum sizes R and W
     */
    public boolean testQuorumSizes() {
        if (qsize < qreadsize) {
            return false;
        }
        if (qsize < qwritesize) {
            return false;
        }
        return true;
    }

    public int getReceivePort() {
        return receivePort;
    }

    public int getWriteQuorum() {
        return qwritesize;
    }

    public int getReadQuorum() {
        return qreadsize;
    }

    public String thisNode() {
        return myNode;
    }

    public HashMap<String, String> getAllHosts(){
        return hosts;
    }

    /**
     * Produces hashmap with host and port information of other nodes 
     * (not including this node)
     * Used later for building channels with other nodes 
     */
    public HashMap<String, String> getOtherNodes(String myNode) {
        HashMap<String, String> otherNodes = new HashMap<String, String>();
        for (String key : hosts.keySet()) {
            if (!myNode.equals(key)) {
                otherNodes.put(key, hosts.get(key));
            }
        }
        return otherNodes;
    }

}
