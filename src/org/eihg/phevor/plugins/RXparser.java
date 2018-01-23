//package org.hci.updb;
package org.eihg.phevor.plugins;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.GraphConvenience.RelTypes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

// Caution - this class assumes the only RX node in Neo4j is the root node with id 0

public class RXparser extends ServerPlugin
{	
	//private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	private static Number get_code(String code_str, int level){
		return level == 4 ?
				Long.parseLong(code_str)
				: Integer.parseInt(code_str);
	}
	
	private static Node create_rx(GraphDatabaseService db, Number child_code, String child_name, String type){
		Node n = db.createNode(Labels.RX);
		n.setProperty("id", child_code);
		n.setProperty("name", child_name);
		n.setProperty("type", type);
		return n;
	}
	
	private static void _parse_record(GraphDatabaseService db, CSVRecord r, Node root, int child_level){
		String type = r.get("CATALOG_TYPE_CODE");
		String child_level_str = Integer.toString(child_level);
		String child_name_label = "CATALOG";
		if(child_level<5) child_name_label += "_HIER"+child_level_str;
		String child_code_label = child_name_label+"_CODE";
		String child_code_str = r.get(child_code_label);
		if(child_code_str.isEmpty()) return;
		Number child_code = get_code(child_code_str, child_level);
		String child_name = r.get(child_name_label);
		Node child = create_rx(db, child_code, child_name, type);
		int parent_level=child_level-1;
		String parent_level_str = Integer.toString(parent_level);
		String parent_name_label = "CATALOG_HIER"+parent_level_str;
		String parent_code_label = parent_name_label+"_CODE";
		String parent_code_str = r.get(parent_code_label);
		if(parent_code_str.isEmpty()){
			child.createRelationshipTo(root, RelTypes.is_a);
			return;
		}
		Number parent_code = get_code(parent_code_str, parent_level);
		Node parent = db.findNode(Labels.RX, "id", parent_code);
		String parent_name = r.get(parent_name_label);
		if(parent != null){
			child.createRelationshipTo(parent, RelTypes.is_a);
			return;
		}
		parent = create_rx(db, parent_code, parent_name, type);
		child.createRelationshipTo(parent, RelTypes.is_a);
	}
	private static void parse_record(GraphDatabaseService db, CSVRecord r, Node root){
		for(int child_level=5; child_level>1; ++child_level){
			_parse_record(db, r, root, child_level);
		}
	}
	
	private static void _parse_rx(GraphDatabaseService db, String in) throws IOException{
		final CSVFormat format = CSVFormat.TDF.withFirstRecordAsHeader();
		try(
				final Reader reader = new FileReader(in);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			int i = 0;
			Transaction tx = db.beginTx();
			final Node root = db.findNode(Labels.RX, "id", 0);
			for (CSVRecord record : records) {
				parse_record(db, record, root);
				++i;
				if(i==99){// batches of 100
					i=0;
					tx.success();
					tx.close();
					tx = db.beginTx();
				}
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
			@Parameter( name = "in_file" ) String in
	) throws IOException{       
		//try(Utility u = Utility.graph_util(db)){
		_parse_rx(db, in);
		return "complete";
		//}
	}
}
