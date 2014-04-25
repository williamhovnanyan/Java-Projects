import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.ConfigurationException;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;


public class StatsUserRating {
	
	public static void main(String[] args) {
		new StatsUserRating().new StatsUserRatingThread(); 
	}
	
	private enum UserCounters { CONNECTION, APPS, FOLLOWERS, FOLLOWING, BLOCKS, MAP_FAILED, REDUCE_FAILED, PHOTOS_COUNT }
	private enum PhotosCounters {  PUBLIC, RECENT, FEATURED, MAP_FAILED, REDUCE_FAILED }
	private enum SummaryCounters { MIN_RATING, MAX_RATING, MAP_FAILED, REDUCE_FAILED }
	
	private static Configuration conf;
	private static final Logger LOG = Logger.getLogger(StatsUserRating.class);
	
    private static void StartJob() throws Exception {
    	
    	conf = HBaseConfiguration.create();
    	
    	HBaseAdmin admin = new HBaseAdmin(conf);
    	
    	try{
    		admin.disableTable("userrating");
    		admin.deleteTable("userrating");
    	} catch(Exception e) {
    		System.err.println("Can't disable or delete table 'userrating' : " + e.toString());
    		e.printStackTrace();
    	}
    	
    	HTableDescriptor desc = new HTableDescriptor("userrating");
    	desc.addFamily(new HColumnDescriptor("s".getBytes()));   	
    	
    	admin.createTable(desc, new RegionSplitter.UniformSplit().split(24));
    	admin.close();
    	
    	Scan userscan = new Scan();
      	userscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("connections"));
      	userscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("apps"));
      	userscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("followers_count"));
    	userscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("following"));
      	userscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("blocks"));
    	userscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("username"));
    	userscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("photos_count"));
    	    	
      	      	
    	userscan.setCaching(1000);
    	userscan.setCacheBlocks(false);
    	
    	setJobConfiguration(conf);
    	
    	Job job = new Job(conf, "UserRating_1");
    	job.setJarByClass(ParseMapper1.class);
    	job.setJarByClass(AnalyzeReducer1.class);
    	    	
    	long startTime = new Date().getTime();
    	
    	TableMapReduceUtil.initTableMapperJob("users", userscan, ParseMapper1.class,  ImmutableBytesWritable.class, IntWritable.class, job);
    	TableMapReduceUtil.initTableReducerJob("userrating", AnalyzeReducer1.class, job);
    	
    	job.waitForCompletion(true);
    	
    	Scan photoscan = new Scan();
    	photoscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("public"));
      	photoscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("recent"));
      	photoscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("featured"));
//     	photoscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("metadata"));
//     	photoscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("comments"));
      	photoscan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("user"));
      	
    	photoscan.setCaching(1000);
    	photoscan.setCacheBlocks(false);
    	
    	job = new Job(conf, "UserRating_2");
    	job.setJarByClass(ParseMapper2.class);
    	job.setJarByClass(AnalyzeReducer2.class);
    	
    	TableMapReduceUtil.initTableMapperJob("photos", photoscan, ParseMapper2.class,  ImmutableBytesWritable.class, IntWritable.class, job);
    	TableMapReduceUtil.initTableReducerJob("userrating", AnalyzeReducer2.class, job);
    	
    	job.waitForCompletion(true);
    	
//    	Scan contestscan = new Scan();
//    	
//    	contestscan.setCaching(1000);
//    	contestscan.setCacheBlocks(false);
//    	
//    	job = new Job(conf, "UserRating_3");
//    	job.setJarByClass(ParseMapper3.class);
//    	job.setJarByClass(AnalyzeReducer3.class);
//    	
//    	TableMapReduceUtil.initTableMapperJob("contests", userscan, ParseMapper3.class,  ImmutableBytesWritable.class, IntWritable.class, job);
//    	TableMapReduceUtil.initTableReducerJob("userrating", AnalyzeReducer3.class, job);
//    	
//    	job.waitForCompletion(true);
    	
    	Scan userratingscan = new Scan();
    	userratingscan.setCaching(1000);
    	userratingscan.setCacheBlocks(false);
    	
    	job = new Job(conf, "UserRating_Summary");
    	job.setJarByClass(ParseMapper4.class);
    	job.setJarByClass(AnalyzeReducer4.class);
    	
    	TableMapReduceUtil.initTableMapperJob("userrating", userratingscan, ParseMapper4.class,  ImmutableBytesWritable.class, IntWritable.class, job);
    	TableMapReduceUtil.initTableReducerJob("userrating", AnalyzeReducer4.class, job);
    	
    	job.waitForCompletion(true);
    	
    	long endTime = new Date().getTime();
    	
    	System.out.println("Min rating is " + job.getCounters().findCounter(SummaryCounters.MIN_RATING).getValue());
    	System.out.println("Max rating is " + job.getCounters().findCounter(SummaryCounters.MAX_RATING).getValue());
    	System.out.println("Job StatsPhoto completed in " + (endTime-startTime)/1000 + " seconds!");
    	
    	try {
    		exportCSV();
    	} catch(Exception e) {
    		System.err.println("Error in exporting CSV");
    		e.printStackTrace();
    	}
    	//System.exit( ? 0 : 1);
  	}
    
   

	private static void setJobConfiguration(Configuration conf) throws Exception {
		// TODO Auto-generated method stub
		PropertiesConfiguration properties = new PropertiesConfiguration("job.properties");
		
		Iterator properIterator = properties.getKeys();
		LOG.info("Job properties are the followings");
		String name = null;
		while (properIterator.hasNext()) {
			try {
				name = (String) properIterator.next();		
				LOG.info(name + " = " + properties.getDouble(name, 0));
				conf.setDouble(name, properties.getDouble(name, 0));
			} catch(Exception e) {
				LOG.error("Error in property " + name, e);
			}
		}			
	}



	private static void ReverseRowKey(byte[] rowkey) throws Exception {
		for (int i = 0; i < rowkey.length/2; i++) {
			byte tmp=rowkey[i];
			rowkey[i]=rowkey[rowkey.length-i-1];
			rowkey[rowkey.length-i-1]=tmp;
		}
	}
    
  	static class ParseMapper1 extends TableMapper<ImmutableBytesWritable, Writable> {

  		private static final Log MapLOGGER = LogFactory.getLog(ParseMapper1.class);
  		private static IntWritable ONE = new IntWritable(1);
  		private static Map<String, Result> APPS = new HashMap<String, Result>(50);
  		
  		@Override
  		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
  				throws IOException, InterruptedException {
  			// TODO Auto-generated method stub
  			HTable appsTable = new HTable(context.getConfiguration(), "apps");
  			
  			Scan scan = new Scan();
  			scan.setCaching(50);
  			
  			ResultScanner scanner = appsTable.getScanner(scan);
  			
  			for (Result result : scanner) {
  				try{
	  				byte[] rowKey = result.getRow().clone();
	  				ReverseRowKey(rowKey);
	  					
					APPS.put(new ObjectId(rowKey).toString(), result);
  				} catch(Exception e) {
  					MapLOGGER.error("Exception in ParseMapper1.setup()", e);
  				}
			} 	
  			appsTable.close();
  			super.setup(context);
  		}
  		
  		
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
			try {
				BasicDBList connections, apps, blocks;
				int followers_count = 0, following_count = 0, photo_count = 0;
				
				byte[] rowkey = row.get().clone();
				ReverseRowKey(rowkey);
				String _id = new ObjectId(rowkey).toString();
				
				try {
					connections = (BasicDBList) JSON.parse(
							new String(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("connections")), "UTF-8"));
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "connections")), 
							new IntWritable(connections.size()));
				} catch(Exception e) {
					MapLOGGER.error("Connection handling exception _id = " + _id, e);
					context.getCounter(UserCounters.CONNECTION).increment(1);
				}
				try {
					apps = (BasicDBList) JSON.parse(
							new String(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("apps")), "UTF-8"));
					
					int paidcount = 0;
					for (Object app : apps.toArray()) {
						String appid = null;
						try
						{
							appid = ((BasicDBObject)app).getString("app");
							Result res = APPS.get(appid);
							
//							System.out.println(objid + " : " + appid + " : " + res);
							
							if(res != null && !res.isEmpty()) {
								BasicDBObject data = (BasicDBObject) JSON.parse(
										new String( res.getColumnLatest(Bytes.toBytes("i"), Bytes.toBytes("data")).getValue(),
										"UTF-8"));
								if(data.getInt("price") > 0) { 
									paidcount++;
								}
							}
						}catch(Exception e){
							MapLOGGER.error("Error in apps col iteration " + e.toString() + " " + appid, e);
						}
					}
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "apps")), 
							new IntWritable(paidcount));
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "apps_count")), 
							new IntWritable(paidcount));
				} catch(Exception e) {
					MapLOGGER.error("Apps handling exception _id = " + _id, e);
					context.getCounter(UserCounters.APPS).increment(1);
				}
				try {
					byte[] fol_count =  columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("followers_count"));
					
					if(fol_count.length == 8)
						followers_count = (int) Bytes.toDouble(fol_count);
					else 
						followers_count = Bytes.toInt(fol_count);
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "follower")), 
							new IntWritable(followers_count));
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "followers_count")), 
							new IntWritable(followers_count));
				} catch(Exception e) {
					MapLOGGER.error("Followers handling exception _id = " + _id, e);
					context.getCounter(UserCounters.FOLLOWERS).increment(1);
				}
				try {
					blocks = (BasicDBList) JSON.parse(
							Bytes.toString(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("blocks"))));
					for (Object object : blocks) {
						context.write(new ImmutableBytesWritable(Bytes.toBytes(object.toString() + ":" + "blocks")), 
								ONE);
					}					
				} catch(Exception e) {
					MapLOGGER.error("Blocks handling exception _id = " + _id, e);
					context.getCounter(UserCounters.BLOCKS).increment(1);
				}
				try {
					BasicDBList following = (BasicDBList) JSON.parse(
							Bytes.toString(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("following"))));
					if(following != null)
						following_count = following.size();
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "following_count")), 
							new IntWritable(following_count));										
				} catch(Exception e) {
					MapLOGGER.error("Following handling exception _id = " + _id, e);
					context.getCounter(UserCounters.FOLLOWING).increment(1);
				}
				try {
					byte[] p_count =  columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("photos_count"));
					
					if(p_count.length == 8)
						photo_count = (int) Bytes.toDouble(p_count);
					else 
						photo_count = Bytes.toInt(p_count);
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "photos_count")), 
							new IntWritable(photo_count));
				} catch(Exception e) {
					MapLOGGER.error("Photos_count handling exception _id = " + _id, e);
					context.getCounter(UserCounters.PHOTOS_COUNT).increment(1);
				}
				try {
					String username = 
							Bytes.toString(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("username")));
					
					if(username != null)				
						context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "username" + ":" + username)), 
								ONE);										
				} catch(Exception e) {
					MapLOGGER.error("Following handling exception _id = " + _id, e);
					context.getCounter(UserCounters.FOLLOWING).increment(1);
				}
			} catch(Exception e) {
				MapLOGGER.error("Error in parsing users information", e);
				context.getCounter(UserCounters.MAP_FAILED).increment(1);
			}
		}
	}
  	
  	static class AnalyzeReducer1	extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
  		private static final Log ReduceLOGGER = LogFactory.getLog(AnalyzeReducer1.class);
    	@Override
		protected void reduce( ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		try {
	    		String[] keys = Bytes.toString(key.get()).split(":");
	    		
	    		if(!keys[1].equals("username")) {
		    		double rating = context.getConfiguration().getDouble(keys[1], 1);
		    		
		    		Integer count = 0;
					Iterator<IntWritable> it = values.iterator();
					while(it.hasNext())	{
						count += ((IntWritable)it.next()).get();
					}
					
					byte[] rowkey = new ObjectId(keys[0]).toByteArray();
					ReverseRowKey(rowkey);
					
					Put put = new Put(rowkey);
					put.add(Bytes.toBytes("s"), Bytes.toBytes(keys[1]), Bytes.toBytes((int)Math.ceil( count.intValue() * rating)));
					
					context.write(new ImmutableBytesWritable(rowkey), put);
	    		} else {
	    			byte[] rowkey = new ObjectId(keys[0]).toByteArray();
					ReverseRowKey(rowkey);
					
					Put put = new Put(rowkey);
					put.add(Bytes.toBytes("s"), Bytes.toBytes(keys[1]), Bytes.toBytes(keys[2]));
					
					context.write(new ImmutableBytesWritable(rowkey), put);
	    		}
    		} catch(Exception e) {
    			ReduceLOGGER.error("Error in counting users information", e);
    			context.getCounter(UserCounters.REDUCE_FAILED).increment(1);
    		}
		}
	}
  	
  	static class ParseMapper2 extends TableMapper<ImmutableBytesWritable, Writable> {
  		private static final Log MapLOGGER = LogFactory.getLog(ParseMapper2.class);
  		private static final IntWritable TRUE = new IntWritable(1);
  		private static final IntWritable FALSE = new IntWritable(0);
  		
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
			try {
				boolean pub, recent, featured;
				
				String _id = Bytes.toString(
						columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("user")));		
				byte[] rowkey = row.get().clone();
				ReverseRowKey(rowkey);
				_id = _id + ":" + new ObjectId(rowkey).toString();
				
				try {
					pub = Bytes.toBoolean(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("public")));
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "public")), 
							pub ? TRUE : FALSE);
				} catch(Exception e) {
					MapLOGGER.error("Public handling exception", e);
					context.getCounter(PhotosCounters.PUBLIC).increment(1);
				}
				try {
					recent = Bytes.toBoolean(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("recent")));
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "recent")), 
							recent ? TRUE : FALSE);
				} catch(Exception e) {
					MapLOGGER.error("Recent handling exception ", e);
					context.getCounter(PhotosCounters.RECENT).increment(1);
				}
				try {
					featured = Bytes.toBoolean(columns.getValue(Bytes.toBytes("i"), Bytes.toBytes("featured")));
					
					context.write(new ImmutableBytesWritable(Bytes.toBytes(_id + ":" + "featured")), 
							featured ? TRUE : FALSE);
				} catch(Exception e) {
					MapLOGGER.error("Featured hanling exception", e);
					context.getCounter(PhotosCounters.FEATURED).increment(1);
				}
			} catch(Exception e) {
				MapLOGGER.error("Error in parsing photos information", e);
				context.getCounter(PhotosCounters.MAP_FAILED).increment(1);
			}
		}
	}
  	
  	static class AnalyzeReducer2	extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
  		private static final Log ReduceLOGGER = LogFactory.getLog(AnalyzeReducer2.class);
    	@Override
		protected void reduce( ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		String[] keys = null;
    		try {
	    		keys = Bytes.toString(key.get()).split(":");
	    		double rating = context.getConfiguration().getDouble(keys[2], 0);
	    		
	    		Integer count = 0;
				Iterator<IntWritable> it = values.iterator();
				while(it.hasNext())	{
					count += ((IntWritable)it.next()).get();
				}
				
				byte[] rowkey = new ObjectId(keys[0]).toByteArray();
				ReverseRowKey(rowkey);
				
				Put put = new Put(rowkey);
				put.add(Bytes.toBytes("s"), Bytes.toBytes(keys[2]), Bytes.toBytes((int)Math.ceil(count.intValue() * rating)));
				
				context.write(new ImmutableBytesWritable(rowkey), put);
    		} catch(Exception e) {
    			ReduceLOGGER.error("Error in counting photos information _id:" + keys[1], e);
    			context.getCounter(PhotosCounters.REDUCE_FAILED).increment(1);
    		}
		}
	}
  	
  	static class ParseMapper3 extends TableMapper<ImmutableBytesWritable, Writable> {
  		private static final Log MapLOGGER = LogFactory.getLog(ParseMapper3.class);
  		private static IntWritable ONE = new IntWritable(1);
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
			
		}
	}
  	
  	static class AnalyzeReducer3	extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
  		private static final Log ReduceLOGGER = LogFactory.getLog(AnalyzeReducer3.class);
    	@Override
		protected void reduce( ImmutableBytesWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			
		}
	}
  	
  	 
	static class ParseMapper4 extends TableMapper<ImmutableBytesWritable, Writable> {
  		private static final Log MapLOGGER = LogFactory.getLog(ParseMapper4.class);
  		
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
			try {
				int summary = 0;
				for (Cell cell : columns.rawCells()) { 
					if(!Bytes.toString(cell.getQualifier()).contains("_count") && 
							!Bytes.toString(cell.getQualifier()).equals("username"))
						summary += Bytes.toInt(cell.getValue());
				}
				
				byte[] rowkey = row.get().clone();
				ReverseRowKey(rowkey);
				MapLOGGER.info(new ObjectId(rowkey).toString() + " has " + summary + " rating");
				
				context.write(row, new IntWritable(summary));
			} catch(Exception e) {
				MapLOGGER.error("Error in counting user's summary rating", e);
				context.getCounter(SummaryCounters.MAP_FAILED).increment(1);
			}
		}
	}
  	
  	static class AnalyzeReducer4 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
  		private static final Log ReduceLOGGER = LogFactory.getLog(AnalyzeReducer4.class);
  		private static int min = 0, max = 0;
  		private static String min_id = null, max_id = null;
    	@Override
		protected void reduce( ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			try {   		
				byte[] rowkey = key.get().clone();
				ReverseRowKey(rowkey);
				String _id = new ObjectId(rowkey).toString();
				
				
	    		Integer count = 0;
				Iterator<IntWritable> it = values.iterator();
				while(it.hasNext())	{
					count += ((IntWritable)it.next()).get();
				}
				if(count.intValue() < min) {
					min = count.intValue();
					min_id = _id;
				}
				if(count.intValue() > max) {
					max = count.intValue();
					max_id = _id;
				}
				
//				determineRange(count.intValue());
				
//				ReduceLOGGER.info("User is " + _id+ " Count is " + count.intValue() + " MIN_RATIN " + 
//						context.getCounter(SummaryCounters.MIN_RATING).getValue() + 
//						" MAX_RATING " + context.getCounter(SummaryCounters.MAX_RATING).getValue());
//				
//				if(count.intValue() < context.getCounter(SummaryCounters.MIN_RATING).getValue())
//					context.getCounter(SummaryCounters.MIN_RATING).setValue(count.intValue());
//				if(count.intValue() > context.getCounter(SummaryCounters.MAX_RATING).getValue()) 
//					context.getCounter(SummaryCounters.MAX_RATING).setValue(count.intValue());
				
				Put put = new Put(key.get());
				put.add(Bytes.toBytes("s"), Bytes.toBytes("summary"), Bytes.toBytes(count.intValue()));
				
				context.write(key, put);
				
			} catch(Exception e) {
				ReduceLOGGER.error("Error in counting user's summary rating", e);
				context.getCounter(SummaryCounters.REDUCE_FAILED).increment(1);
			}
		}
    	
    	private void determineRange(int intValue) {
			// TODO Auto-generated method stub
			
		}
    	
		@Override
    	protected void cleanup(
    			org.apache.hadoop.mapreduce.Reducer.Context context)
    			throws IOException, InterruptedException {
    		// TODO Auto-generated method stub
    		ReduceLOGGER.info("Min is " + min + ", User is " + min_id +"\nMax is " + max + ", User is " + max_id);
    		LOG.info("Min is " + min + ", User is " + min_id +"\nMax is " + max + ", User is " + max_id);    		
    		super.cleanup(context);
    	}
	}
  	
  	static void exportCSV() throws Exception {
		try {
			System.out.println("Exporting into /root/Statistics/UsersRatingAnalytics/stats.csv");
			
			Configuration conf = HBaseConfiguration.create();
			HTable userrating = new HTable(conf, "userrating");
						
			FileWriter writer = new FileWriter("/root/Statistics/UsersRatingAnalytics/stats.csv");
			
			writer.append("username");
			writer.append(',');
			writer.append("apps_count");
			writer.append(',');
			writer.append("followers_count");
			writer.append(',');
			writer.append("following_count");
			writer.append(',');
			writer.append("photos_count");
			writer.append(',');
			writer.append("rating");
			writer.append('\n');
			
			Scan scan = new Scan();
			scan.setCaching(1000);
			
			scan.addColumn(Bytes.toBytes("s"), Bytes.toBytes("username"));
			scan.addColumn(Bytes.toBytes("s"), Bytes.toBytes("followers_count"));
			scan.addColumn(Bytes.toBytes("s"), Bytes.toBytes("following_count"));
			scan.addColumn(Bytes.toBytes("s"), Bytes.toBytes("apps_count"));
			scan.addColumn(Bytes.toBytes("s"), Bytes.toBytes("photos_count"));
			scan.addColumn(Bytes.toBytes("s"), Bytes.toBytes("rating"));
			
			ResultScanner scanner = userrating.getScanner(scan);
			int count = 0;
			for (Result result : scanner) {
				
				if(result.containsColumn(Bytes.toBytes("s"), Bytes.toBytes("username")))
					writer.append(new String(result.getValue(Bytes.toBytes("s"), Bytes.toBytes("username")), "UTF-8"));
				else 
					writer.append("");
				writer.append(',');
				
				if(result.containsColumn(Bytes.toBytes("s"), Bytes.toBytes("apps_count")))
					writer.append(String.valueOf(Bytes.toInt(result.getValue(Bytes.toBytes("s"), Bytes.toBytes("apps_count")))));
				else 
					writer.append("");
				writer.append(',');
				
				if(result.containsColumn(Bytes.toBytes("s"), Bytes.toBytes("followers_count")))
					writer.append(String.valueOf(Bytes.toInt(result.getValue(Bytes.toBytes("s"), Bytes.toBytes("followers_count")))));
				else 
					writer.append("");
				writer.append(',');
				
				if(result.containsColumn(Bytes.toBytes("s"), Bytes.toBytes("following_count")))
					writer.append(String.valueOf(Bytes.toInt(result.getValue(Bytes.toBytes("s"), Bytes.toBytes("following_count")))));
				else 
					writer.append("");
				writer.append(',');
				
				if(result.containsColumn(Bytes.toBytes("s"), Bytes.toBytes("photos_count")))
					writer.append(String.valueOf(Bytes.toInt(result.getValue(Bytes.toBytes("s"), Bytes.toBytes("photos_count")))));
				else 
					writer.append("");
				writer.append(',');
				
				if(result.containsColumn(Bytes.toBytes("s"), Bytes.toBytes("summary")))
					writer.append(String.valueOf(Bytes.toInt(result.getValue(Bytes.toBytes("s"), Bytes.toBytes("summary")))));
				else 
					writer.append("");
				writer.append(',');
				
				writer.append('\n');
				count++;
				if(count%2000==0)
					System.out.println("Rows inserted : "+count);
			}
			System.out.println("Rows inserted : " + count);
			writer.flush();
			writer.close();
			userrating.close();
			
		} catch(Exception e) {
			System.out.println("Exception "+e.toString());
			e.printStackTrace();
		}
	}
  	
  	private class StatsUserRatingThread implements  Runnable {
  		
  		Thread t;
  		public StatsUserRatingThread() {
  			t = new Thread(this, "StatsUserRating");
  			t.start();
  		}
  		
  		@Override
  		public void run() {
  			// TODO Auto-generated method stub
  			System.out.println("In StatsUserRating.run");
  			
  			try {
  				StatsUserRating.StartJob();
  			} catch(Exception e){
  				System.out.println("Exception on thread StatsUserRating "+e.toString());
  			}
  			
  		}
  	}
}

    

