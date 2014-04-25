package com.picsart.neo4j.DB;

import java.util.Iterator;

import javax.naming.ConfigurationException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.PropertyConfigurator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;



public class DBDumper {
	private static String DB_PATH = "/storage/neo4j/graph.db/";
	private static String pathToConfig = "/var/lib/neo4j/conf/";
	
	private static enum RelTypes implements RelationshipType 
	{
		FOLLOWS
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		GraphDatabaseService graphDb = new GraphDatabaseFactory()
			.newEmbeddedDatabaseBuilder(DB_PATH)
			.loadPropertiesFromFile(pathToConfig + "neo4j.properties")
			.newGraphDatabase();
		registerShutdownHook(graphDb);
		
		Configuration configs = null;
		boolean userreindex = false;
		boolean photoreindex = false;
		
		String configFileName = "configuration.properties";
				
		for (int i = 0; i < args.length; i++) {
			if(args[i].equalsIgnoreCase("--config"))
				configs = new PropertiesConfiguration(configFileName);
		}
		if(configs == null)
			configs = new PropertiesConfiguration(configFileName);
		
		userreindex = configs.getBoolean("userreindex", false);
		String mongoHost = configs.getString("mongo-host1", "127.0.0.1");
		int mongoPort = configs.getInt("mongo-port1", 27017);
		
		
		if(userreindex) {
			MongoContentDumper dbdumper = new MongoContentDumper(mongoHost, mongoPort, graphDb);
			dbdumper.start();
		}
		
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// TODO Auto-generated method stub
		//Shutdown when the current JVM exits. 
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				graphDb.shutdown();
			}
		});
	}

}
