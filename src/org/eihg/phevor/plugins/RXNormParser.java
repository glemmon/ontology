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
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.Relationships;
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.ReadsRelationshipsWithTypes;
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
		String aui1_str = record.get("RXAUI1").trim();
		String aui2_str = record.get("RXAUI2").trim();
		if(aui1_str.isEmpty()) return;
		if(aui2_str.isEmpty()) return;
		int aui1 = Integer.parseInt(aui1_str);
		int aui2 = Integer.parseInt(aui2_str);
		Node n1 = db.findNode(Labels.Atom, "aui", aui1);
		Node n2 = db.findNode(Labels.Atom, "aui", aui2);
		String rui = record.get("RUI").trim();
		String rel = record.get("REL").trim();
		String rela = record.get("RELA").trim();
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
		Node atom = db.createNode(Labels.Atom, Labels.valueOf(sab));
		atom.setProperty("aui", aui);	
		atom.setProperty("tty", tty);
		atom.setProperty("name", name);
		Node concept = db.findNode(Labels.Concept, "cui", cui);
		if(concept==null){
			concept = db.createNode(Labels.Concept);
			concept.setProperty("cui", cui);
			concept.setProperty("name", name);
		}else if(sab=="RXNORM") concept.setProperty("name", name);
		
		atom.createRelationshipTo(concept, RelTypes.is_a);
	}
	
	private static void _parse_rx(GraphDatabaseService db, String nodes, String relations) throws IOException{
		final CSVFormat format = CSVFormat.TDF.withFirstRecordAsHeader();
		Transaction tx = db.beginTx();
		//// Process Nodes ////
		try(
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
		}
		//// Process Relationships ////
		try(
				final Reader reader = new FileReader(relations);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			int i = 0;
			tx = db.beginTx();
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
