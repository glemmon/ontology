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

public class RXNormParser extends ServerPlugin
{	
	//private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	
	private static void process_relation(GraphDatabaseService db, CSVRecord record){
		Integer aui1 = Integer.parseInt( record.get("RXAUI1"));
		Integer aui2 = Integer.parseInt( record.get("RXAUI2"));
		String rui = record.get("RUI").trim();
		String rel = record.get("REL").trim();
		String rela = record.get("RELA").trim();
		Node n1 = db.findNode(Labels.RX, "aui", aui1);
		Node n2 = db.findNode(Labels.RX, "aui", aui2);
		Relationship r;
		if(rela.isEmpty())
			r = n1.createRelationshipTo(n2, RelTypes.valueOf(rel));
		else
			r = n1.createRelationshipTo(n2, RelTypes.valueOf(rela));
		r.setProperty("rel", rel);
		if(!rui.isEmpty()) r.setProperty("rui", Integer.parseInt(rui));
	}
	
	private static void process_node(GraphDatabaseService db, CSVRecord record){
		Integer aui = Integer.parseInt( record.get("RXAUI"));
		Integer cui = Integer.parseInt( record.get("RXCUI"));
		String sab = record.get("SAB");
		String tty = record.get("TTY");
		String name = record.get("STR");
		Node n = db.createNode(Labels.RX, Labels.valueOf(sab));
		n.setProperty("aui", aui);
		n.setProperty("cui", cui);
		n.setProperty("tty", tty);
		n.setProperty("name", name);
	}
	
	private static void _parse_rx(GraphDatabaseService db, String nodes, String relations) throws IOException{
		final CSVFormat format = CSVFormat.TDF.withFirstRecordAsHeader();
		Transaction tx = db.beginTx();
		//// Process Nodes ////
		/*try(
				final Reader reader = new FileReader(nodes);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			int i = 0;
			for (CSVRecord record : records) {
				process_node(db, record); //, root);
				++i;
				if(i==999){// batches of 100
					i=0;
					tx.success();
					tx.close();
					tx = db.beginTx();
				}
				//logger.info(Integer.toString(i)+" "+record.get("aui"));
			}
			
			tx.success();
			tx.close();
		}*/
		//// Process Relationships ////
		try(
				final Reader reader = new FileReader(relations);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			int i = 0;
			//tx = db.beginTx();
			for (CSVRecord record : records) {
				process_relation(db, record); //, root);
				++i;
				if(i==999){// batches of 100
					i=0;
					tx.success();
					tx.close();
					tx = db.beginTx();
				}
				//logger.info(Integer.toString(i)+" "+record.get("rui"));
			}
			
			tx.success();
			tx.close();
		}
	}
	
	@Description( "Add ICD->CCS relations" )
	@PluginTarget( GraphDatabaseService.class )
	public String parse_rx(
			@Source GraphDatabaseService db,            
			@Description( "Input File" )
			@Parameter( name = "node_file" ) String nodes,
			@Description( "Relation File" )
			@Parameter( name = "relation_file" ) String relations
	) throws IOException{       
		_parse_rx(db, nodes, relations);
		return "complete";
	}
}
