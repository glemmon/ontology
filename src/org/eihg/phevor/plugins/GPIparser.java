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
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

public class GPIparser extends ServerPlugin
{	
	private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	
	private static Object get_code(String code_str){
		try{
			return Integer.parseInt(code_str);
		}catch(NumberFormatException e){}
		try{
			return Long.parseLong(code_str);
		}catch(NumberFormatException e){
			return code_str;
		}
	}
	
	private static Node create_rx(GraphDatabaseService db, int level, Object code, String name, Integer type_code, String type){
		Node n = db.createNode(Labels.GPI);
		n.setProperty("level", level);
		n.setProperty("id", code);
		n.setProperty("name", name);
		if(type_code != null) n.setProperty("type_code", type_code);
		if(! type.isEmpty()) n.setProperty("type", type);
		return n;
	}
	
	private static Pair<Node, Boolean> get_item(GraphDatabaseService db, CSVRecord r){
		String dwid_str = r.get("ITEM_DWID");
		Object dwid = get_code(dwid_str);
		Node item = db.findNode(Labels.ITEM, "dwid", dwid);
		if(item!=null) return Pair.of(item,false);
		item = db.createNode(Labels.ITEM);
		item.setProperty("dwid", dwid);
		String id_str = r.get("ITEM_CODE");
		Object id = get_code(id_str);
		item.setProperty("id", id);
		item.setProperty("name", r.get("ITEM"));
		return Pair.of(item,true);
	}
	
	private static Node find_catalog(GraphDatabaseService db, int level, Object code){
		ResourceIterator<Node> nodes = db.findNodes(Labels.GPI, "id", code);
		while(nodes.hasNext()){
			Node n = nodes.next();
			if( n.getProperty("level", 0).equals(level) ){
				return n;
			}
		}
		return null;
	}
	
	private static Node get_catalog(GraphDatabaseService db, CSVRecord r, int level, Integer type_code, String type){
		String level_str = Integer.toString(level);
		String name_label = "CATALOG";
		if(level<5) name_label += "_HIER"+level_str;
		String code_label = name_label+"_CODE";
		String code_str = r.get(code_label);
		if(code_str.isEmpty()) return null;
		Object code = get_code(code_str);
		//Object child_code = get_code(code_str);
		Node found = find_catalog(db, level, code);
		if(found != null) return found;
		String name = r.get(name_label);
		return create_rx(db, level, code, name, type_code, type);
	}
	
	// Item connected, continue
	private static Pair<Boolean,Boolean> _parse_record(GraphDatabaseService db, CSVRecord r, Node root, Node item, int child_level){
		String type_code_str = r.get("CATALOG_TYPE_CODE");
		Integer type_code = type_code_str.isEmpty() ? null : Integer.parseInt(type_code_str);
		String type = r.get("CATALOG_TYPE");
		Node child = get_catalog(db, r, child_level, type_code, type);
		if(child==null) return Pair.of(false, true);
		boolean item_connected = false;
		if(item != null){
			item.createRelationshipTo(child, RelTypes.is_a);
			item_connected = true;
		}
		if(child.hasRelationship(RelTypes.is_a, Direction.OUTGOING)){
			// For now, ASSUME children have only 1 parent
			return Pair.of(item_connected, false);
		}
		if( child_level == 1){
			child.createRelationshipTo(root, RelTypes.is_a);
			return Pair.of(item_connected, false);
		}
		int parent_level=child_level-1;
		Node parent = get_catalog(db, r, parent_level, type_code, type);
		if(parent == null){
			child.createRelationshipTo(root, RelTypes.is_a);
			return Pair.of(item_connected, false);
		}
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
	
	private static Node find_root(GraphDatabaseService db){
		ResourceIterator<Node> roots = db.findNodes(Labels.ROOT);
		while(roots.hasNext()){
			Node n = roots.next();
			if( n.hasLabel(Labels.GPI)){
				return n;
			}
		}
		return null;
	}
	
	private static void _parse_rx(GraphDatabaseService db, String in) throws IOException{
		final CSVFormat format = CSVFormat.TDF.withFirstRecordAsHeader();
		Transaction tx = db.beginTx();
		//// Process Nodes ////
		try(
				final Reader reader = new FileReader(in);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			int i = 0;
			final Node root = find_root(db);
			if(root == null) throw new AssertionError("root test");
			for (CSVRecord record : records) {
				parse_record(db, record, root);
				++i;
				if(i==999){// batches of 100
					i=0;
					tx.success();
					tx.close();
					tx = db.beginTx();
				}
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
			@Description( "Each row contains catalog hierarchy for item" )
			@Parameter( name = "gpi_file" ) String nodes
	) throws IOException{       
		_parse_rx(db, nodes);
		return "complete";
	}
}
