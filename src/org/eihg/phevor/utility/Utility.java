package org.eihg.phevor.utility;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;

public class Utility implements AutoCloseable{
	//private static final String default_db_path = "/home/glemmon/UPDB/data/neo4j-2.2/data/graph.db";
	//private static final String default_config_path = "/home/glemmon/UPDB/data/neo4j-2.2/neo4j.properties";
	
	private GraphDatabaseService graph_db = null;
	private Transaction tx = null;

	private static Utility utility = null;
	
	private Utility(GraphDatabaseService db){
		graph_db = db;
		tx = graph_db.beginTx();
	}
	
	private Utility(String db_path, String config_path){
		this(new GraphDatabaseFactory()
		.newEmbeddedDatabaseBuilder(db_path)
		.loadPropertiesFromFile(config_path)
		.newGraphDatabase()	
		);	
	}
	
	//private Utility(String db_path){
	//	this(db_path, default_config_path);
	//}
	
	//private Utility(){
	//	this(default_db_path, default_config_path);
	//}
	
	public static Utility graph_util(GraphDatabaseService db){
		if(utility == null) utility = new Utility(db);
		return utility;
	}
	
	public static Utility graph_util(String db_path, String config_path){
		if(utility == null) utility = new Utility(db_path, config_path);
		return utility;
	}
	
	//public static Utility graph_util(String db_path){
	//	if(utility == null) utility = new Utility(db_path);
	//	return utility;
	//}
	
	public static Utility graph_util(){
		assert( utility != null);
		return utility;
	}
	
	public GraphDatabaseService get_graph(){
		return graph_db;
	}
	
	//public void success(){ tx.success(); }
	
	public void close(){
		tx.success();
		tx.close();
		utility = null;
	}
	
	public void registerShutdownHook(){
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( 
	    	new Thread(){
		        @Override
		        public void run()
		        {
		        	tx.success();
		        	graph_db.shutdown();
		        }
		    } 
	    );
	}
	
	public static void registerShutdownHook( final BatchInserter inserter ){
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( 
	    	new Thread(){
		        @Override
		        public void run()
		        {
		        	inserter.shutdown();
		        }
		    } 
	    );
	}
	
	static int count(Iterable<?> iterable) {
		return count(iterable.iterator());
	}
	
	static int count(Iterator<?> iterator) {
		return IteratorUtil.count(iterator);
	}
}
