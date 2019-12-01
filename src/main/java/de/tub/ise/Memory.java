package de.tub.ise;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Methods to store key and retrieve key-value pairs from memory
 * Not pretty but does the trick ¯\_(ツ)_/¯
 */
public class Memory {
	
	static Logger logger = Logger.getLogger(Memory.class.getName());

	private static ConcurrentHashMap<String, String> memory = new ConcurrentHashMap<String, String>();
	
	public static void put(String key, String value) {
		memory.put(key, value);
		logger.info(key + " -> " + value);
	}
	
	public static String get(String key) {
		return memory.get(key);
	}
	
	public static String delete(String key) {
		return memory.remove(key);
	}
	
}