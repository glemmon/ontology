//package org.hci.updb;
package org.eihg.phevor.plugins;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

//import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.GraphConvenience.RelTypes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

public class Item2RXNormParser extends ServerPlugin
{	
	//private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	
	private static void process_relation(GraphDatabaseService db, CSVRecord record){
		String item_dwid_str = record.get("ITEM_DWID").trim();
		String rxcui_norm_str = record.get("RXCUI_NORM").trim();
		if(item_dwid_str.isEmpty()) return;
		if(rxcui_norm_str.isEmpty()) return;
		int item_dwid = Integer.parseInt(item_dwid_str);
		int rxcui_norm = Integer.parseInt(rxcui_norm_str);
		Node n1 = db.findNode(Labels.ITEM, "dwid", item_dwid);
		if(n1==null) throw new AssertionError("Item missing: "+item_dwid_str);
		Node n2 = db.findNode(Labels.Concept, "cui", rxcui_norm);
		if(n2==null) return; // Concept missing - retired.
		n1.createRelationshipTo(n2, RelTypes.is_a);
	}
	
	private static void _item2rxnorm(GraphDatabaseService db, String nodes) throws IOException{
		final CSVFormat format = CSVFormat.TDF.withFirstRecordAsHeader();
		Transaction tx = db.beginTx();
		//// Process Nodes ////
		try(
				final Reader reader = new FileReader(nodes);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			int i = 0;
			for (CSVRecord record : records) {
				process_relation(db, record);
				++i;
				if(i==999){// batches of 1000
					i=0;
					tx.success();
					tx.close();
					tx = db.beginTx();
				}
				//logger.info(Integer.toString(i)+" "+record.get("aui"));
			}
			
			tx.success();
			tx.close();
		}
	}
	
	@Description( "Add ICD->CCS relations" )
	@PluginTarget( GraphDatabaseService.class )
	public String item2rxnorm(
			@Source GraphDatabaseService db,            
			@Description( "File with item to RXNorm mapping" )
			@Parameter( name = "in_file" ) String nodes
	) throws IOException{       
		_item2rxnorm(db, nodes);
		return "complete";
	}
}
