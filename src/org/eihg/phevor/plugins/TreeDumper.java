//package org.hci.updb;
package org.eihg.phevor.plugins;

import org.eihg.phevor.utility.GraphConvenience;
import org.eihg.phevor.utility.Utility;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.graphdb.Path;
import org.neo4j.helpers.collection.Iterables;

public class TreeDumper extends ServerPlugin
{
	//private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 
	
	//public TreeDumper(){
	//	super();
		//logger.setLevel(Level.ALL);
	//}

	private static Label get_onto_label(Node node){
		Iterable<Label> labels = node.getLabels();
		for(Label label : labels){
			if(label == GraphConvenience.Labels.ROOT) continue;
			return label;
		}
		assert(false); 
		return null;
	}
	
	private static Node find_root(Label label){
		ResourceIterator<Node> root_nodes =  GraphConvenience.get_roots();
		StringBuilder sb = new StringBuilder();
		while(root_nodes.hasNext()){
			Node root_node = root_nodes.next();
			Label onto_label = get_onto_label(root_node);
			sb.append(root_node);
			sb.append(onto_label);
			if(onto_label.equals(label)) return root_node;
			
		}
		throw new RuntimeException("No matching label :"+label+sb.toString());
	}
	
	private static String path_to_mtree(Path path){
		return String.join(".", 
			Iterables.map(n->n.getProperty("id").toString(), path.nodes())
		);
	}
	
	// Assumes graph_util(db) has already been called
	private static String traverse(String label_str){
		Label label = Labels.valueOf(label_str);
		Node root = find_root(label);
		return String.join("\n",
			Iterables.map(TreeDumper::path_to_mtree, GraphConvenience.get_descendant_paths(root))
		);
	}
	
	@Description( "Find ROOT associated with this label and return the tree in 'mtree' format" )
	@PluginTarget( GraphDatabaseService.class )
	public String dump_tree(
			@Source GraphDatabaseService db,            
			@Description( "There should be one node with this label and the ROOT label" )
			@Parameter( name = "label" ) String query
	){       
		try(Utility u = Utility.graph_util(db)){
			return traverse(query);
		}
	}
}
