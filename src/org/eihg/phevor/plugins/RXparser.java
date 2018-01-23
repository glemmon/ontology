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
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

public class RXparser extends ServerPlugin
{	
	//private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	private static Number get_code(String code_str, int level){
		return level == 4 ?
				Long.parseLong(code_str)
				: Integer.parseInt(code_str);
	}
	
	private static Node create_rx(GraphDatabaseService db, int level, Number code, String name, String type){
		Node n = db.createNode(Labels.RX);
		n.setProperty("level", level);
		n.setProperty("id", code);
		n.setProperty("name", name);
		n.setProperty("type", type);
		return n;
	}
	
	private static Node get_item(GraphDatabaseService db, CSVRecord r){
		String dwid_str = r.get("ITEM_DWID");
		int dwid = Integer.parseInt(dwid_str);
		Node item = db.findNode(Labels.RX, "dwid", dwid);
		if(item!=null) return item;
		item = db.createNode(Labels.RX);
		String id_str = r.get("ITEM_CODE");
		try{
			int id = Integer.parseInt(id_str);
			item.setProperty("id", id);
		}catch(NumberFormatException e){}
		item.setProperty("name", r.get("ITEM"));
		return item;
	}
	
	private static Node find_catalog(GraphDatabaseService db, int level, Number code){
		ResourceIterator<Node> nodes = db.findNodes(Labels.RX, "id", code);
		while(nodes.hasNext()){
			Node n = nodes.next();
			int other_level = (int)n.getProperty("level", 0);
			if( other_level == level ){
				return n;
			}
		}
		return null;
	}
	
	private static boolean _parse_record(GraphDatabaseService db, CSVRecord r, Node root, Node item, int child_level){
		String type = r.get("CATALOG_TYPE_CODE");
		String child_level_str = Integer.toString(child_level);
		String child_name_label = "CATALOG";
		if(child_level<5) child_name_label += "_HIER"+child_level_str;
		String child_code_label = child_name_label+"_CODE";
		String child_code_str = r.get(child_code_label);
		if(child_code_str.isEmpty()) return false;
		Number child_code = get_code(child_code_str, child_level);
		Node child = find_catalog(db, child_level, child_code);
		String child_name = r.get(child_name_label);
		if(child == null){
			child = create_rx(db, child_level, child_code, child_name, type);
		}
		boolean item_connected = false;
		if(item != null){
			item.createRelationshipTo(child, RelTypes.is_a);
			item_connected = true;
		}
		int parent_level=child_level-1;
		String parent_level_str = Integer.toString(parent_level);
		String parent_name_label = "CATALOG_HIER"+parent_level_str;
		String parent_code_label = parent_name_label+"_CODE";
		String parent_code_str = r.get(parent_code_label);
		if(parent_code_str.isEmpty()){
			child.createRelationshipTo(root, RelTypes.is_a);
			return item_connected;
		}
		Number parent_code = get_code(parent_code_str, parent_level);
		Node parent = find_catalog(db, parent_level, parent_code);
		String parent_name = r.get(parent_name_label);
		if(parent == null){
			parent = create_rx(db, parent_level, parent_code, parent_name, type);
		}
		child.createRelationshipTo(parent, RelTypes.is_a);
		return item_connected;
	}
	private static void parse_record(GraphDatabaseService db, CSVRecord r, Node root){
		Node item = get_item(db, r);
		for(int child_level=5; child_level>1; ++child_level){
			boolean item_connected = _parse_record(db, r, root, item, child_level);
			if( item_connected ) item = null;
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
