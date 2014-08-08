package com.bigstep;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.ServerAddress;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import java.text.Normalizer;
import java.security.MessageDigest;

import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

 
import java.io.Serializable;
import java.io.File;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;



import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
 
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
 
public class MongoSampler extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final org.apache.log.Logger log = LoggingManager.getLoggerForClass();
    private MongoClient client=null;
    private DB db=null;
    private DBCollection collection=null;
 
    // set up default arguments for the JMeter GUI
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("method", "GET");
        defaultParameters.addArgument("queryservers", "127.0.0.1:27019");
        defaultParameters.addArgument("shardservers", "127.0.0.1:27017");
        defaultParameters.addArgument("db", "mydb");
        defaultParameters.addArgument("collection", "mycoll");
        defaultParameters.addArgument("username", "");
        defaultParameters.addArgument("password", "");

        defaultParameters.addArgument("key", "");
        defaultParameters.addArgument("value", "");
        return defaultParameters;
    }
   	
    @Override 
    public void setupTest(JavaSamplerContext context)
    {
	try
	{
		String queryservers = context.getParameter( "queryservers" );
		String shardservers = context.getParameter( "shardservers");

		String dbname = context.getParameter( "db" );
		String collectionname = context.getParameter( "collection" );

		String username = context.getParameter( "username" );
		String password = context.getParameter( "password" );

		String method = context.getParameter( "method" );


		List<ServerAddress> hosts= new ArrayList<ServerAddress>();
		
		String[] arrServers=queryservers.split(","); 
		for(String server: arrServers)
		{
			String parts[]=server.split(":");
			hosts.add(new ServerAddress(parts[0],Integer.parseInt(parts[1])));
		}

		client = new MongoClient(hosts);

		client.setWriteConcern(WriteConcern.JOURNALED);

		Thread.sleep(5000);

		arrServers=shardservers.split(","); 
		for(String server: arrServers)
		{
			//add the shard servers
			client.getDB("admin").command(new BasicDBObject("addshard", server));
		}

		Thread.sleep(5000);
	
		//enable sharding on our db	
		client.getDB("admin").command(new BasicDBObject("enablesharding", dbname));
		
		final BasicDBObject shardKey = new BasicDBObject("myid", 1);
	        shardKey.put("hash", 1);
		
		//specify which collection is sharded and speficy which key name to use for hash	
		final BasicDBObject cmd = new BasicDBObject("shardcollection", dbname+"."+collectionname);
		cmd.put("key", shardKey);	
		
		//set the shard key on that specific collection
		client.getDB("admin").command(cmd);

		Thread.sleep(1000);

		db = client.getDB( dbname );	
		collection = db.getCollection( collectionname );			
		if(collection==null)
			throw new Exception("Collection not initialized!");

		
	}
	catch(Exception ex)
	{
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            ex.printStackTrace( new java.io.PrintWriter( stringWriter ) );

	     log.error("setupTest:"+ex.getMessage()+stringWriter.toString());
 
	}
			
    }
    
    @Override	
    public void teardownTest(JavaSamplerContext context)
    {
	if(null!=client)
		client.close();	
    }

    private byte [] md5(final String pValue) throws Exception
    { return MessageDigest.getInstance("MD5").digest(pValue.getBytes("UTF-8")); }
 
    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        String key = context.getParameter( "key" );
        String value = context.getParameter( "value" );
        String method = context.getParameter( "method" );
        
	SampleResult result = new SampleResult();
        result.sampleStart(); // start stopwatch
	DBCursor cursor=null;


	
        try {
	   if(collection==null)
		throw new Exception("Collection not initialized!");



	    if(null==client)
			throw new Exception("Mongo Client not initialised");

	    if(method.equals("GET"))	
	    {
		   BasicDBObject query = new BasicDBObject("myid", key);
	 	   cursor=collection.find(query);
		   cursor.next();
	    }
	    else 
		if(method.equals("PUT"))
		{
			if(value!="")
			{
				
				String nvalue = Normalizer.normalize(value, Normalizer.Form.NFC);	
				DBObject dbObject = (DBObject) JSON.parse(nvalue);
				dbObject.put("myid",(Object)key);
				dbObject.put("hash",md5(key));
				collection.insert(dbObject);	
			}
		}
		else
		if (method.equals("QUERY"))
		{
		}
	
    
            result.sampleEnd(); // stop stopwatch
            result.setSuccessful( true );
            result.setResponseMessage( "OK on object "+key );
            result.setResponseCodeOK(); // 200 code

        } catch (Exception e) {
            result.sampleEnd(); // stop stopwatch
            result.setSuccessful( false );
            result.setResponseMessage( "Exception: " + e );
 
            // get stack trace as a String to return as document data
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            e.printStackTrace( new java.io.PrintWriter( stringWriter ) );
            result.setResponseData( stringWriter.toString() );
            result.setDataType( org.apache.jmeter.samplers.SampleResult.TEXT );
            result.setResponseCode( "500" );

	    log.error("runTest: "+e.getClass()+" "+e.getMessage()+" "+stringWriter.toString());
        }
	finally
	{
	 if(cursor!=null)
	   cursor.close();
	}
 
        return result;
    }
}

