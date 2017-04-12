//package org.hci.updb;
package org.eihg.phevor.plugins;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.*;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;

import org.apache.commons.lang3.StringUtils;
import org.eihg.phevor.utility.GraphConvenience;
import org.eihg.phevor.utility.Utility;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.graphdb.Path;

public class TextQuery extends ServerPlugin
{
	private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	
	private static Map<Label,Node> roots = null;

	public TextQuery(){
		super();
		logger.setLevel(Level.ALL);
	}

	private static Iterable<Node> find_matching_nodes(String query, boolean hp_only){
		String label = hp_only ? ":HP" : "";
		String cypher = "MATCH (n"+label+") WHERE n.full_name =~ "'+ query +"' RETURN n";
		Result result = Utility.graph_util().get_graph().execute( cypher );
		Iterator<Node> n_column = result.columnAs( "n" );
		return Iterators.asIterable( n_column );
	}

	private static Node find_root(Label label){
		if(roots == null){
			roots = new HashMap<>();
			
			ResourceIterator<Node> root_nodes =  GraphConvenience.get_roots();
			while(root_nodes.hasNext()){
				Node root_node = root_nodes.next();
				Label onto_label = get_onto_label(root_node);
				roots.put(onto_label, root_node);
			}	
		}
		
		return roots.get(label);
	}
	
	private static JsonObjectBuilder node_to_json(Node node, Label label){
		JsonObjectBuilder object_builder = Json.createObjectBuilder();
		try{
			object_builder.add("id", node.getId());
			object_builder.add("label", label.toString());
			object_builder.add("title", node.getProperty("full_name").toString());
		}catch(Exception e){
			String error = e.toString()+"\n";
			error += node.toString()+",";
			for(String s : node.getPropertyKeys()) error += s + ",";
			error += "\n";
			object_builder.add("ERROR", error);
		}
		return object_builder;
	}

	private static JsonArrayBuilder path_to_json(Path path, Label label){
		//logger.info("Path1: "+path.toString());
		JsonArrayBuilder array_builder = Json.createArrayBuilder();
		//TODO use path.reverseNodes() after Neo4j team debugs it.
		for(Node node : path.nodes()){	
			//logger.info("Node1: " + node.toString());
			array_builder.add(node_to_json(node, label));
		}
		return array_builder;
	}

	private static Label get_onto_label(Node node){
		Iterable<Label> labels = node.getLabels();
		for(Label label : labels){
			if(label.name().equals( GraphConvenience.Labels.ROOT.name() ) ) 
				continue;
			return label;
		}
		assert(false); 
		return null;
	}
	
	// Assumes graph_util(db) has already been called
	private static JsonArrayBuilder collect_paths(String query, boolean hp_only){
		JsonArrayBuilder array_builder = Json.createArrayBuilder();

		for( Node result : find_matching_nodes(query, hp_only)){
			Label label = get_onto_label(result);
			Node root = find_root(label);
			//TODO traverser framework to collect nodes faster than shortest path?
			PathExpander<?> expander = GraphConvenience.get_expander("up");
			PathFinder<Path> shortestPath = GraphAlgoFactory.shortestPath( expander, 1000 );
			for ( Path path : shortestPath.findAllPaths( result, root ) ){ // Wrong direction but this should be faster since its a tree
				array_builder.add(path_to_json(path,label));
			}
		}
		return array_builder;
	}

	// Assumes graph_util(db) has already been called
	private static JsonArrayBuilder collect_nodes(String query, boolean hp_only){
		JsonArrayBuilder array_builder = Json.createArrayBuilder();

		for( Node result : find_matching_nodes(query, hp_only)){
			Label label = get_onto_label(result);
			array_builder.add(node_to_json(result,label));
		}
		return array_builder;
	}

	@Description( "Find nodes with matching text query, return paths from root to node" )
	@PluginTarget( GraphDatabaseService.class )
	public String find_paths(
			@Source GraphDatabaseService db,            
			@Description( "Text query for onto term search" )
			@Parameter( name = "query" ) String query,
			@Parameter( name = "hp_only", optional = true ) Boolean hp_only
			){       
	
		JsonArrayBuilder paths = null;
		if (hp_only == null) hp_only = false;

		try(Utility u = Utility.graph_util(db)){
			paths = collect_paths(query, hp_only);
		}
	
		return paths.build().toString();
	}
	
	@Description( "Find nodes with matching text query, return paths from root to node" )
	@PluginTarget( GraphDatabaseService.class )
	public String find_nodes(
			@Source GraphDatabaseService db,            
			@Description( "Text query for onto term search" )
			@Parameter( name = "query" ) String query,
			@Parameter( name = "hp_only", optional = true ) Boolean hp_only
			){       
	
		JsonArrayBuilder nodes = null;
		if (hp_only == null) hp_only = false;
	
		try(Utility u = Utility.graph_util(db)){
			nodes = collect_nodes(query, hp_only);
		}
	
		return nodes.build().toString();
	}
}
