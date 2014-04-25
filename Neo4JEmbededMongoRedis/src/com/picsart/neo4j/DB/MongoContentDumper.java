package com.picsart.neo4j.DB;

import java.util.Date;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.helpers.collection.IteratorUtil;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.util.JSON;

public class MongoContentDumper {

	private String mongoHost;
	private int mongoPort;
	private MongoClient mongoClient = null;
	private DB PICSART = null;
	private DBCollection collection = null;
	private GraphDatabaseService graphDb = null;
	
	private static final Label userLabel = new Label() {
		
		@Override
		public String name() {
			// TODO Auto-generated method stub
			return "user";
		}
	};
	
	private static final Logger LOG = Logger.getLogger(MongoContentDumper.class);
	
	private static enum RelTypes implements RelationshipType {
		FOLLOWS
	}
	
	public MongoContentDumper(String host, int port, GraphDatabaseService graphDb) throws Exception {
		// TODO Auto-generated constructor stub
		mongoHost = host;
		mongoPort = port;
		LOG.info("Connecting to " + mongoHost + ":" + port + " ..." + " for dumping database");
	       
		mongoClient = new MongoClient(mongoHost, mongoPort);
		mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
		PICSART = mongoClient.getDB("PICSART");
		
		collection = PICSART.getCollection("users");
		this.graphDb = graphDb;
		try(Transaction tx = this.graphDb.beginTx()) 
		{			
			AutoIndexer<Node> nodeautoindexer = graphDb.index().getNodeAutoIndexer();
			nodeautoindexer.setEnabled(true);
			nodeautoindexer.startAutoIndexingProperty("name");
			nodeautoindexer.startAutoIndexingProperty("username");
			nodeautoindexer.startAutoIndexingProperty("id");
			nodeautoindexer.startAutoIndexingProperty("_id");
			nodeautoindexer.startAutoIndexingProperty("created");
			
//			RelationshipAutoIndexer relautoindexer = graphDb.index().getRelationshipAutoIndexer();
//			relautoindexer.setEnabled(true);
//			relautoindexer.startAutoIndexingProperty("first_user");
//			relautoindexer.startAutoIndexingProperty("second_user");
			
			tx.success();
		}
	}
	
	public void start() {
		
//		System.out.println("Starting to dump mongo at " + new Date().toString());
//		startDumping();
//		System.out.println("Dump finished at " + new Date().toString());
		System.out.println("Starting to create followings at " + new Date().toString());
		createFollowingRelations();
		System.out.println("Following creatin finished at " + new Date().toString());
	}
	
	private void startDumping() {
		// TODO Auto-generated method stub
		DBCursor cursor = collection.find().sort(new BasicDBObject("$natural", 1));
        cursor.addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
        
		DBObject obj;
		long counter = 0;
		cursor.batchSize(1000);
		
		Transaction tx = graphDb.beginTx();
		while( cursor.hasNext() )
		{	
			try {
				obj = cursor.next();
			
				if(obj.get("name") == null || obj.get("username") == null
						|| obj.get("created") == null)
					continue;
								
				Node node = graphDb.createNode(userLabel);
				node.setProperty("name", obj.get("name"));
				node.setProperty("username", obj.get("username"));
				node.setProperty("created", ((Date)obj.get("created")).getTime());
				node.setProperty("id", obj.get("id").toString());
				node.setProperty("_id", obj.get("_id").toString());
				
				
				
				BasicDBList follist = new BasicDBList();
				if(obj.get("following")!=null)
					for (Object object : (BasicDBList)obj.get("following")) {
						follist.add(object.toString());
					}
				
				node.setProperty("following", follist.toString());
				
				BasicDBList photos = new BasicDBList();
				if(obj.get("photos")!=null)
					for (Object object : (BasicDBList)obj.get("photos")) {
						photos.add(object.toString());
					}
				
				node.setProperty("photos", photos.toString());
				
				BasicDBList blocks = new BasicDBList();
				if(obj.get("blocks")!=null)
					for (Object object : (BasicDBList)obj.get("blocks")) {
						blocks.add(object.toString());
					}
				
				node.setProperty("blocks", blocks.toString());
				
				if(obj.get("provider")!=null)
					node.setProperty("provider", obj.get("provider"));
				
				if(obj.get("photos_count")!=null)
					node.setProperty("photos_count", ((Number)obj.get("photos_count")).intValue());
				
				if(obj.get("streams_count")!=null)
					node.setProperty("streams_count", ((Number)obj.get("streams_count")).intValue());
				
				if( obj.get("status")!=null)
					node.setProperty("status", obj.get("status"));
				
				if(obj.get("top")!=null)
					node.setProperty("top", obj.get("top"));
				
				if(obj.get("twitter")!=null)
					if(((DBObject)obj.get("twitter")).containsField("id") 
							&& ((DBObject)obj.get("twitter")).get("id")!=null)
						node.setProperty("twitter_id", ((Number)((DBObject)obj.get("twitter")).get("id")).longValue());
					
				if(obj.get("facebook")!=null)
					if(((DBObject)obj.get("facebook")).containsField("id") 
							&& ((DBObject)obj.get("facebook")).get("id")!=null)
						node.setProperty("facebook_id", ((Number)((DBObject)obj.get("facebook")).get("id")).longValue());
				
				if(obj.get("updated")!=null)
					node.setProperty("updated", ((Date)obj.get("updated")).getTime());
				
				if(obj.get("location")!=null) {
					BasicDBObject location = (BasicDBObject) obj.get("location");
					for (Object object : location.keySet()) {
						if(!object.toString().equals("coordinates") && location.get(object.toString())!=null)
							node.setProperty("location:" + object.toString(), location.getString(object.toString()));
					}
					BasicDBList coords = (BasicDBList) location.get("coordinates");
					if(coords != null && coords.size() == 2) {
						node.setProperty("location:long", coords.get(0));
						node.setProperty("location:lat", coords.get(1));
					}						
				}
				
				if(obj.get("following_count")!=null)
					node.setProperty("following_count", ((Number)obj.get("following_count")).intValue());
				
				if(obj.get("followers_count")!=null)
					node.setProperty("followers_count", ((Number)obj.get("followers_count")).intValue());
				
				if(obj.get("email")!=null)
					node.setProperty("email", obj.get("email"));
				
				if(obj.get("blacklist")!=null)
					node.setProperty("blacklist", obj.get("blacklist"));
				
				if(obj.get("apps")!=null && ((BasicDBList)obj.get("apps")).size() > 0)
					node.setProperty("apps", obj.get("apps").toString());
				
				if(counter%500 == 0) {
					tx.success();					
				}
				if(counter%10000 == 0) {
					tx.close();
					tx = graphDb.beginTx();
				}
				counter++;
			} catch(Exception e) {
				LOG.log(Priority.ERROR, "Exception while coping users, details ", e);
			}
			
			if(counter%1000 == 0)
				LOG.info(counter + " are copied!!!!!!!!!!!!!!!");
		}
		
		try {
			tx.success();
			tx.close();
		} catch(Exception e) {
			LOG.log(Priority.ERROR, "Exception in closing transaction, details ", e);
		}
	}


	private void createFollowingRelations() {
		// TODO Auto-generated method stub
		Transaction tx = graphDb.beginTx();
		long nodecounter = 0;
		long relcounter = 0;
		try {
			Iterable<Node> it = graphDb.getAllNodes();
			ExecutionEngine engine = new ExecutionEngine(graphDb);
			
			
			for (Node node : it) {	
				
				long start = System.currentTimeMillis();
				BasicDBList follist = null;
				try {
					follist = (BasicDBList)	JSON.parse((String) node.getProperty("following"));
				} catch(Exception e) {
					LOG.error("Error while creating following list", e);
				}
				if(follist == null)
					continue;
//				Map<String, Object> params = new HashMap<String, Object>();
//				params.put("phparr", follist);
//				LOG.info("Follist " + follist.toString());
				
				long querybegin = System.currentTimeMillis();
//				ExecutionResult result = 
//						//engine.execute("start n=node(*) where (n._id in {phparr}) return n", params);
//						engine.execute("start n=node:node_auto_index(id=\"" + node.getProperty("id") + "\") match (n._id in " + follist.toString() + ") return n");		
//				LOG.info("Query for node " + node.getProperty("id") + " took " + (System.currentTimeMillis() - querybegin) + " millis.");
//				Iterator<Node> n_column = result.columnAs("n");
				
//				long iterBegin = System.currentTimeMillis();
//				long begin = System.currentTimeMillis();
//				for (Node secNode : IteratorUtil.asIterable(n_column)) {
//					LOG.info("An iteration for second Node node " + secNode.getId() + " took " + (System.currentTimeMillis() - begin) + " millis."); 
//					Relationship rel = node.createRelationshipTo(secNode, RelTypes.FOLLOWS);
//					rel.setProperty("first_node", node.getProperty("id"));
//					rel.setProperty("second_node", node.getProperty("id"));
//					relcounter++;
//					begin = System.currentTimeMillis();
//				}
//				LOG.info("An iteration for node " + node.getProperty("id") + " took " + (System.currentTimeMillis() - iterBegin) + " millis.");
				for (Object object : follist) {
					
					long begin = System.currentTimeMillis();
					//ResourceIterable<Node> secNodeIt = graphDb.findNodesByLabelAndProperty(userLabel, "_id", object.toString());
					ExecutionResult result = 
							engine.execute("start n=node:node_auto_index(_id=\"" + object.toString() + "\") return n;");
					
					Iterator<Node> secNodeIt = result.columnAs("n");
				    	
					if(secNodeIt.hasNext()) {
						Node secondNode = secNodeIt.next();
							
//						LOG.info("Getting second Node takes " + (System.currentTimeMillis() - begin) + " millis");
						
						begin = System.currentTimeMillis();
						Relationship rel = node.createRelationshipTo(secondNode, RelTypes.FOLLOWS);
//					    rel.setProperty("first_user", node.getProperty("id"));
//					    rel.setProperty("second_user", secondNode.getProperty("id"));
					    relcounter++;
//					    LOG.info("Nodes are " + node.getId() + " " + secondNode.getId() + ", relation is " + rel.getId());
					}
//				    LOG.info("Node processed " + nodecounter + ", relation created " + relcounter);
				}
				try {
					node.removeProperty("following");
				} catch(Exception e) {
					LOG.error("Exception in removing property of node " + node.getProperty("_id"), e);
				}
				nodecounter++;
				if(nodecounter % 50 == 0) {
					tx.success();
					LOG.info(relcounter + " relations for " + nodecounter + " nodes are created!" );
				}
				if(nodecounter % 10000 == 0) {
					tx.close();
					tx = graphDb.beginTx();
				}
				
				LOG.info("Processing one node takes " + (System.currentTimeMillis() - start) + " millis");
				LOG.info("Number of processed nodes : " + nodecounter + ", Number of created relations " + relcounter);
				try {
					if(System.currentTimeMillis() - start > 10000)
						LOG.info("Node " + node.getProperty("id") + " has  " + ((BasicDBList)JSON.parse((String)node.getProperty("following"))).size() + " followings");
				} catch(Exception e) {
					LOG.error("Exception in removing property of node " + node.getProperty("_id"), e);
				}
			}
		} catch(Exception e) {
			LOG.log(Priority.ERROR, "Exception while creating following relations, details ", e);
		} finally {
			tx.success();
			tx.close();
		}		
		LOG.info("Finished!!\n" + relcounter + " relations for " + nodecounter + " nodes are created!" );
	}

}
