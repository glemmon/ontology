//package org.hci.updb;
package org.eihg.phevor.plugins;

import org.apache.commons.lang3.BooleanUtils;
import org.eihg.phevor.utility.GraphConvenience;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.Utility;
import static org.eihg.phevor.utility.GraphConvenience.Labels.*;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.helpers.collection.Iterables;

public class TreeDumper extends ServerPlugin
{
	private static Map<Label, Short> padding = ImmutableMap.<Label, Short>builder()
			.put(OD,(short)6)
			//.put(CHEBI, NULL) // CHEBI is NOT padded
			.put(CO,(short)6)
			.put(DOID,(short)7)
			.put(GO,(short)7)
			//.put(HGNC, NULL) // HGNC is NOT padded
			.put(HP,(short)7)
			.put(MP,(short)7)
			//.put(NCBI_GENE, NULL) // NCBI_GENEs are not padded
			//.put(ICD9dx, NULL) // ICD9dx are not padded
			//.put(ICD10dx, NULL) // ICD10dx are not padded
			.build();

	private static Label get_onto_label(Node node){
		Iterable<Label> labels = node.getLabels();
		for(Label label : labels){
			if(label.name().equals( ROOT.name() )) continue;
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
			if(onto_label.name().equals(label.name())) return root_node;
			
		}
		throw new RuntimeException("No matching label :"+label+sb.toString());
	}
	
	private static String format_id(Label label, Node n, boolean pad){
		String id = n.getProperty("id").toString();
		if(! pad) return id;
		if(! padding.containsKey(label)) return id;
		return Strings.padStart(id, padding.get(label), '0');
	}
	
	private static String path_to_mtree(Label l, Path path, boolean pad){
		return String.join("/", 			
			Iterables.map(n->format_id(l,n,pad), path.nodes())
		) +" "+	path.endNode().getProperty("name", "").toString();
	}
	
	// Assumes graph_util(db) has already been called
	private static String traverse(String label_str, boolean pad){
		Label label = Labels.valueOf(label_str);
		Node root = find_root(label);
		ResourceIterable<Node> children = GraphConvenience.get_children(root); // Parents terminology adopted from persons not really applicable here
		return String.join("\n",
			Iterables.map(n->TreeDumper.path_to_mtree(label,n,pad), GraphConvenience.get_descendant_paths(children))
		);
	}
	
	@Description( "Find ROOT associated with this label and return the tree in 'mtree' format" )
	@PluginTarget( GraphDatabaseService.class )
	public String dump_tree(
			@Source GraphDatabaseService db,            
			@Description( "There should be one node with this label and the ROOT label" )
			@Parameter( name = "label" ) String query,
			@Description( "Whether to pad output with zeros" )
			@Parameter( name = "pad", optional = true ) Boolean pad

	){       
		try(Utility u = Utility.graph_util(db)){
			return traverse(query, BooleanUtils.toBoolean(pad));
		}
	}
}
