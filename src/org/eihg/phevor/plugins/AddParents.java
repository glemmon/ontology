//package org.hci.updb;
package org.eihg.phevor.plugins;

import org.eihg.phevor.utility.GraphConvenience;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.Utility;

import java.util.List;
import java.util.Arrays;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

import org.neo4j.helpers.collection.Iterables;

public class AddParents extends ServerPlugin
{
	private static Node id2node(GraphDatabaseService db, String id, Label onto){
		final Node n = db.findNode(onto, "id", id);
		if(n==null){System.out.println("NO ID: "+id); throw new RuntimeException("No ID: "+id);}
		return n;
	}
	
	private static String node2id(Node n){
		return (String) n.getProperty("long_id");
	}
	
	// Assumes graph_util(db) has already been called
	private static String _add_parents(GraphDatabaseService db, String children_str, String onto_str){
		Label onto = Labels.valueOf(onto_str);
		List<String> children_strs = Arrays.asList(children_str.split("\\s*,\\s*"));
		Iterable<Node> children = Iterables.map(c->id2node(db,c,onto), children_strs);
		// parents to include children
		Iterable<Node> ascendants = GraphConvenience.get_ascendants(children, true);
		Iterable<String> ascendants_str = Iterables.map(AddParents::node2id, ascendants);
		return String.join(",", ascendants_str);
	}
	
	@Description( "Find ROOT associated with this label and return the tree in 'mtree' format" )
	@PluginTarget( GraphDatabaseService.class )
	public String add_parents(
			@Source GraphDatabaseService db,            
			@Description( "Comma separated list of child IDs" )
			@Parameter( name = "children" ) String query,
			@Description( "Ontology from which to search" )
			@Parameter( name = "ontology" ) String onto

	){       
		try(Utility u = Utility.graph_util(db)){
			return _add_parents(db, query, onto);
		}
	}
}
