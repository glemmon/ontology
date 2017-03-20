package org.eihg.phevor.utility;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.tooling.GlobalGraphOperations;

public class GraphConvenience {
	private static final int default_depth = 1000;
	private static int node_count = -1; // We assume a read-only DB
	
	public static enum Labels implements Label {
		OD,
		CHEBI,
		CO,
		DOID,
		GO,
		HGNC,
		HP,
		MP,
		NCBI_GENE,
		ICD9dx,
		ICD10dx,
		ROOT
	}
	
	public static enum RelTypes implements RelationshipType
	{
	    is_a,
	    part_of,
	    binds_gene
	}
	
	private static PathExpander<?> ascendants_path = PathExpanders.forTypesAndDirections(RelTypes.is_a, Direction.OUTGOING, RelTypes.part_of, Direction.OUTGOING);
	private static PathExpander<?> descendants_path = PathExpanders.forTypesAndDirections(RelTypes.is_a, Direction.INCOMING, RelTypes.part_of, Direction.INCOMING);
	private static PathExpander<?> ascend_descend_path = PathExpanders.forTypesAndDirections(RelTypes.is_a, Direction.BOTH, RelTypes.part_of, Direction.BOTH);
	private static PathExpander<?> binds_gene_path = PathExpanders.forTypeAndDirection(RelTypes.binds_gene, Direction.OUTGOING);

	public static PathExpander<?> get_expander(String direction){
		if(direction.equals("up")) return ascendants_path;
		else if (direction.equals("down")) return descendants_path;
		else if (direction.equals("both")) return ascend_descend_path;
		else throw new InvalidParameterException();
	}
	
	//public static Iterator<Node> node_sample(int sample_size, Predicate p){
	//	int node_count = node_count();
	//}
	
	//Have to use 2 PathExpanders, one up and one down.
	public static ArrayList<Integer> path_lengths_through_common_descent(Node n1, Node n2, Integer max_depth){
        max_depth = max_depth==null ? default_depth : max_depth;
		ArrayList<Integer> path_lengths = new ArrayList<>();
		HashMap<Node,int[]> ancestor_lengths = new HashMap<>();
		for(Path path1 : get_ascendant_paths(n1)){
			int[] lengths = {path1.length()};
			ancestor_lengths.merge(path1.endNode(), lengths, (x,y) -> ArrayUtils.addAll(x,y)); // Java 1.8 Lambda
        }
       	for(Path path2 : get_ascendant_paths(n2)){
    		if(ancestor_lengths.containsKey(path2.endNode())){
    			for(int length : ancestor_lengths.get(path2.endNode())){
    				path_lengths.add(length+path2.length());
    			}
    		}
    	}
		return path_lengths;
	}
	
	public static int node_count(){
		if(node_count == -1){
			node_count = Utility.count(GlobalGraphOperations.at(Utility.graph_util().get_graph()).getAllNodes());
		}
		return node_count;
	}
	
	//static ResourceIterable<Node> get_all_nodes(){
	//	return GlobalGraphOperations.at(Utility.graph_util().get_graph()).getAllNodes();
	//}

	//public static ResourceIterator<Node> get_all_hp(){
	//	return Utility.graph_util().get_graph().findNodes(Labels.HP);
	//}
	
	//public static ResourceIterator<Node> get_all_ncbi_genes(){
	//	return Utility.graph_util().get_graph().findNodes(Labels.NCBI_GENE);
	//}

	
	//public static ResourceIterator<Node> get_all_hgnc(){
	//	return Utility.graph_util().get_graph().findNodes(Labels.HGNC);
	//}
	
	public static ResourceIterator<Node> get_roots(){
		return Utility.graph_util().get_graph().findNodes( Labels.ROOT );
	}

	//public static ResourceIterable<Node> get_subgraph_nodes(Node node){
	//	TraversalDescription sub_graph = Utility.graph_util().get_graph().traversalDescription()
	//			.expand(PathExpanders.allTypesAndDirections());
	//	return get_traversed_nodes(node, sub_graph);	
	//}
	
	static ResourceIterable<Path> get_ascendant_paths(Node node){
		TraversalDescription ascendants = get_ascendants_traversal_description();
		return get_traversed_paths(node, ascendants);	
	}
	
	public static ResourceIterable<Path> get_descendant_paths(Node node){
		TraversalDescription ascendants = get_descendants_traversal_description();
		return get_traversed_paths(node, ascendants);	
	}
	
	static ResourceIterable<Node> get_ascendants(Node node){
		TraversalDescription ascendants = get_ascendants_traversal_description();
		return get_traversed_nodes(node, ascendants);	
	}
	
	static ResourceIterable<Node> get_descendants(Node node){
		TraversalDescription descendants = get_descendants_traversal_description();
		return get_traversed_nodes(node, descendants);
	}
	
	public static ResourceIterable<Node> get_genetic_relatives(Node node, Integer depth){
		depth = depth==null ? default_depth : depth;
		TraversalDescription relatives = get_genetic_relatives_traversal_description(depth);
		return get_traversed_nodes(node, relatives);
	}
	
	//static int count_ascendants(Node node){
	//	return Utility.count(get_ascendants(node));
	//}
	
	//public static int count_descendants(Node node){
	//	return Utility.count(get_descendants(node));
	//}

	static int count_parents(Node node){
		int parents=0;
		parents += node.getDegree(RelTypes.is_a, Direction.OUTGOING);
		parents += node.getDegree(RelTypes.part_of, Direction.OUTGOING);
		return parents;		
	}
	
	public static int count_children(Node node){
		int children=0;
		// Obviously a person can't be a dad and a mom, but this is easier than checking gender
		children += node.getDegree(RelTypes.is_a, Direction.INCOMING);
		children += node.getDegree(RelTypes.part_of, Direction.INCOMING);
		return children;
	}
	
	public static TraversalDescription get_parent_traversal_description(){
		return Utility.graph_util().get_graph().traversalDescription()
				.evaluator(Evaluators.toDepth(1))
				.evaluator(Evaluators.excludeStartPosition())
				.expand(ascendants_path);
	}
	
	public static TraversalDescription get_ascendants_traversal_description(){
		return Utility.graph_util().get_graph().traversalDescription()
				.evaluator(Evaluators.excludeStartPosition())
				.expand(ascendants_path);
	}
	
	// Including Self!
	public static TraversalDescription get_genetic_relatives_traversal_description(Integer depth){
		return Utility.graph_util().get_graph().traversalDescription()
				.evaluator(Evaluators.toDepth(depth))
				.expand(ascend_descend_path);
	}
	
	public static ResourceIterable<Node> get_parents(Node node){
		return get_traversed_nodes(node,get_parent_traversal_description());
	}
	
	public static TraversalDescription get_children_traversal_description(){
		return Utility.graph_util().get_graph().traversalDescription()
				.evaluator(Evaluators.toDepth(1))
				.evaluator(Evaluators.excludeStartPosition())
				.expand(descendants_path);
	}
	
	public static ResourceIterable<Node> get_children(Node node){
		return get_traversed_nodes(node,get_children_traversal_description());
	}
	
	public static TraversalDescription get_descendants_traversal_description(){
		return Utility.graph_util().get_graph().traversalDescription()
				.evaluator(Evaluators.excludeStartPosition())
				.expand(descendants_path);
	}
	
	static private ResourceIterable<Node> get_traversed_nodes(Node node, TraversalDescription description){
		return description.traverse(node).nodes();
	}
	
	public static ResourceIterable<Path> get_traversed_paths(Node node, TraversalDescription description){
		return description.traverse(node);
	}
}
