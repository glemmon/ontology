//package org.hci.updb;
package org.eihg.phevor.plugins;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
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
	private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	private static Number get_code(String code_str, int level){
		return level == 4 ?
				Long.parseLong(code_str)
				: Integer.parseInt(code_str);
	}
	
	private static Node create_rx(GraphDatabaseService db, int level, Number code, String name, int type_code, String type){
		Node n = db.createNode(Labels.RX);
		n.setProperty("level", level);
		n.setProperty("id", code);
		n.setProperty("name", name);
		n.setProperty("type_code", type_code);
		n.setProperty("type", type);
		return n;
	}
	
	private static Pair<Node, Boolean> get_item(GraphDatabaseService db, CSVRecord r){
		String dwid_str = r.get("ITEM_DWID");
		int dwid = Integer.parseInt(dwid_str);
		Node item = db.findNode(Labels.RX, "dwid", dwid);
		if(item!=null) return Pair.of(item,false);
		item = db.createNode(Labels.RX);
		item.setProperty("dwid", dwid);
		String id_str = r.get("ITEM_CODE");
		try{
			int id = Integer.parseInt(id_str);
			item.setProperty("id", id);
		}catch(NumberFormatException e){
			item.setProperty("id", id_str);
		}
		item.setProperty("name", r.get("ITEM"));
		return Pair.of(item,true);
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
	
	private static Pair<Node,Boolean> get_catalog(GraphDatabaseService db, CSVRecord r, int level, int type_code, String type){
		String level_str = Integer.toString(level);
		String name_label = "CATALOG";
		if(level<5) name_label += "_HIER"+level_str;
		String code_label = name_label+"_CODE";
		String code_str = r.get(code_label);
		if(code_str.isEmpty()) return null;
		Number child_code = get_code(code_str, level);
		Node child = find_catalog(db, level, child_code);
		if(child != null) return Pair.of(child, false);
		String child_name = r.get(name_label);
		child = create_rx(db, level, child_code, child_name, type_code, type);
		return Pair.of(child, true);
	}
	
	// Item connected, continue
	private static Pair<Boolean,Boolean> _parse_record(GraphDatabaseService db, CSVRecord r, Node root, Node item, int child_level){
		String type_code_str = r.get("CATALOG_TYPE_CODE");
		int type_code = Integer.parseInt(type_code_str);
		String type = r.get("CATALOG_TYPE");
		Pair<Node, Boolean> child_created = get_catalog(db, r, child_level, type_code, type);
		if(child_created==null) return Pair.of(false, true);
		Node child = child_created.getLeft();
		boolean created = child_created.getRight();
		boolean item_connected = false;
		if(item != null){
			item.createRelationshipTo(child, RelTypes.is_a);
			item_connected = true;
		}
		if(!created) return Pair.of(item_connected, false);
		if( child_level == 1){
			child.createRelationshipTo(root, RelTypes.is_a);
			return Pair.of(item_connected, false);
		}
		int parent_level=child_level-1;
		Pair<Node, Boolean> parent_created = get_catalog(db, r, parent_level, type_code, type);
		if(parent_created == null){
			child.createRelationshipTo(root, RelTypes.is_a);
			return Pair.of(item_connected, false);
		}
		Node parent = parent_created.getLeft();
		child.createRelationshipTo(parent, RelTypes.is_a);
		return Pair.of(item_connected, true);
	}
	private static void parse_record(GraphDatabaseService db, CSVRecord r, Node root){
		Pair<Node,Boolean> item_created = get_item(db, r);
		if(item_created == null) throw new AssertionError("item_created shouldn't be null");
		if(!item_created.getRight()) return; // Already processed
		Node item = item_created.getLeft();
		for(int child_level=5; child_level>0; --child_level){
			Pair<Boolean, Boolean> itemconnected_continue = _parse_record(db, r, root, item, child_level);
			if(itemconnected_continue == null) throw new AssertionError("test1");
			boolean stop = ! itemconnected_continue.getRight();
			if(stop) break;
			boolean item_connected = itemconnected_continue.getLeft();
			if(item != null && item_connected) item = null;
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
			if(root == null) throw new AssertionError("root test");
			for (CSVRecord record : records) {
				parse_record(db, record, root);
				++i;
				//if(i==99){// batches of 100
					//i=0;
					tx.success();
					tx.close();
					tx = db.beginTx();
				//}
				logger.info(Integer.toString(i)+" "+record.get("ITEM_DWID"));
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
		_parse_rx(db, in);
		return "complete";
	}
}
