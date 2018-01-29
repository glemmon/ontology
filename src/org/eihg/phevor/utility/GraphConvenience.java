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
import org.neo4j.helpers.collection.Iterables;

public class GraphConvenience {
	private static final int default_depth = 1000;
	private static long node_count = -1; // We assume a read-only DB
	
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
		ROOT, 
		CCSdx,
		CCSpx, 
		Diagnosis,
		Section,
		Chapter,
		ITEM,
		Concept,
		Atom,
		GPI,
		GS,
		MDDB,
		ATC,
		MMSL,
		VANDF,
		MTHSPL,
		MSH,
		NDDF,
		NDFRT_FDASPL,
		MTHCMSFRF,
		MMX,
		NDFRT,
		RXNORM,
		SNOMEDCT_US,
		NDFRT_FMTSME
	}
	
	public static enum RelTypes implements RelationshipType
	{
	    is_a,
	    part_of,
	    binds_gene,
	    inverse_isa,
	    has_print_name,
	    has_product_component,
	    active_metabolites_of,
	    constitutes,
	    consists_of,
	    doseformgroup_of,
	    mapped_from,
	    ingredient_of,
	    print_name_of,
	    has_inactive_ingredient,
	    includes,
	    may_be_prevented_by,
	    has_contraindicating_mechanism_of_action,
	    has_therapeutic_class,
	    contains,
	    permuted_term_of,
	    has_permuted_term,
	    sort_version_of,
	    mapped_to,
	    inactive_ingredient_of,
	    has_sort_version,
	    contraindicating_mechanism_of_action_of,
	    precise_ingredient_of,
	    has_ingredients,
	    has_quantified_form,
	    has_active_ingredient,
	    isa,
	    dose_form_of,
	    has_dose_form,
	    may_be_treated_by,
	    physiologic_effect_of,
	    contraindicating_physiologic_effect_of,
	    contraindicated_with_disease,
	    has_physiologic_effect,
	    effect_may_be_inhibited_by,
	    has_precise_ingredient,
	    has_doseformgroup,
	    contained_in,
	    active_moiety_of,
	    has_entry_version,
	    product_component_of,
	    mechanism_of_action_of,
	    has_active_metabolites,
	    induced_by,
	    metabolic_site_of,
	    has_contraindicating_physiologic_effect,
	    has_form,
	    included_in,
	    has_mechanism_of_action,
	    has_contraindicated_drug,
	    pharmacokinetics_of,
	    induces,
	    has_chemical_structure,
	    form_of,
	    reformulation_of,
	    reformulated_to,
	    active_ingredient_of,
	    entry_version_of,
	    has_contraindicating_class,
	    therapeutic_class_of,
	    may_treat,
	    may_diagnose,
	    has_pharmacokinetics,
	    may_inhibit_effect_of,
	    site_of_metabolism,
	    has_member,
	    member_of,
	    has_tradename,
	    has_ingredient,
	    has_active_moiety,
	    chemical_structure_of,
	    may_be_diagnosed_by,
	    contraindicating_class_of,
	    may_prevent,
	    tradename_of,
	    has_part,
	    ingredients_of,
	    quantified_form_of,
	    RN,
	    PAR,
	    RB,
	    SY,
	    RO,
	    SIB,
	    CHD
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
		for(Path path1 : get_ascendant_paths(n1, false)){
			int[] lengths = {path1.length()};
			ancestor_lengths.merge(path1.endNode(), lengths, (x,y) -> ArrayUtils.addAll(x,y)); // Java 1.8 Lambda
        }
       	for(Path path2 : get_ascendant_paths(n2, false)){
    		if(ancestor_lengths.containsKey(path2.endNode())){
    			for(int length : ancestor_lengths.get(path2.endNode())){
    				path_lengths.add(length+path2.length());
    			}
    		}
    	}
		return path_lengths;
	}
	
	public static long node_count(){
		if(node_count == -1){
			ResourceIterable<Node> ri = Utility.graph_util().get_graph().getAllNodes();
			node_count = Iterables.count(ri);
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
	
	static ResourceIterable<Path> get_ascendant_paths(Node node, boolean include_self){
		TraversalDescription ascendants = get_ascendants_traversal_description(include_self);
		return get_traversed_paths(node, ascendants);	
	}
	
	public static ResourceIterable<Path> get_descendant_paths(Node node){
		TraversalDescription descendant = get_descendants_traversal_description();
		return get_traversed_paths(node, descendant);	
	}
	
	public static ResourceIterable<Path> get_descendant_paths(Iterable<Node> nodes) {
		TraversalDescription descendant = get_descendants_traversal_description();
		return get_traversed_paths(nodes, descendant);	
	}
	
	static ResourceIterable<Node> get_ascendants(Node node, boolean include_self){
		TraversalDescription ascendants = get_ascendants_traversal_description(include_self);
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
	
	public static TraversalDescription get_ascendants_traversal_description(boolean include_self){
		TraversalDescription td =  Utility.graph_util().get_graph().traversalDescription()
				.expand(ascendants_path);
		return include_self ? td : td.evaluator(Evaluators.excludeStartPosition());
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
	
	public static ResourceIterable<Node> get_ascendants(Iterable<Node> children, boolean include_self) {
		return get_traversed_nodes(children,get_ascendants_traversal_description(include_self));

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
				.expand(descendants_path)
				.evaluator(Evaluators.excludeStartPosition())
				;
	}
	
	static private ResourceIterable<Node> get_traversed_nodes(Node node, TraversalDescription description){
		return description.traverse(node).nodes();
	}
	
	static private ResourceIterable<Node> get_traversed_nodes(Iterable<Node> nodes, TraversalDescription description){
		return description.traverse(nodes).nodes();
	}
	
	static private ResourceIterable<Path> get_traversed_paths(Node node, TraversalDescription description){
		return description.traverse(node);
	}
	
	static private ResourceIterable<Path> get_traversed_paths(Iterable<Node> nodes, TraversalDescription description){
		return description.traverse(nodes);
	}
}
