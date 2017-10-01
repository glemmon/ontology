//package org.hci.updb;
package org.eihg.phevor.plugins;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.GraphConvenience.RelTypes;
//import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

public class ICDtoCCS extends ServerPlugin
{	
	private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 

//	private static boolean has_rel(Node ccs, Node icd10){
//		for( Relationship r : icd10.getRelationships(Direction.OUTGOING, RelTypes.is_a)){
//			if( icd10.equals(r.getEndNode() ))
//				return true;
//		}
//		return false;
//	}
	
	private static Node find_icd10(GraphDatabaseService db, String icd10_id, String name){
		String corrected_id;
		String section_id=null;
		if( icd10_id.length() > 3){
			char[] icd10_chrs = icd10_id.toCharArray();
			int index = 3;
			section_id = new String(icd10_chrs, 0, index);
			int diag_length = Math.max(icd10_chrs.length - index, 3);
			String diag = new String(icd10_chrs, index, diag_length);
			corrected_id = section_id+'.'+diag;
		}else corrected_id = icd10_id;
		
		ResourceIterator<Node> node_itr = db.findNodes(Labels.ICD10dx, "id", corrected_id);
		List<Node> nodes = IteratorUtils.toList(node_itr);
		node_itr.close();
		if(nodes.size()==1) return nodes.get(0);
		for(Node n : nodes) 	if(n.hasLabel(Labels.Diagnosis)) return n;
		for(Node n : nodes) 	if(n.hasLabel(Labels.Section)) return n;
		for(Node n : nodes) 	if(n.hasLabel(Labels.Chapter)) return n;
		if( corrected_id.length() <= 3 ) return null;// Can't create chapters and sections here
		Node n = db.createNode(Labels.ICD10dx, Labels.Diagnosis);
		n.setProperty("id", corrected_id);
		n.setProperty("name", name);
		Iterator<Node> section_itr = new FilteringIterator<>(
				db.findNodes(Labels.ICD10dx, "id", section_id),
				node->node.hasLabel(Labels.Section)
		);
		List<Node> sections = IteratorUtils.toList(section_itr);
		if(sections.size()!=1) throw new RuntimeException("multi-sectid: "+section_id);
		n.createRelationshipTo(sections.get(0), RelTypes.is_a);
		return n;
	}
	
	private static String get_sans_quotes(Map<String,String> m, String k){
		return m.get(k).replaceAll("'","");
	}
	
	private static void _icd10_to_ccs(GraphDatabaseService db, String in) throws IOException{
		final CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
		try(
				final Reader reader = new FileReader(in);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			String last_ccs_long_id = null;
			Node last_ccs = null;
			for (CSVRecord record : records) {
				Transaction tx = db.beginTx();
				Map<String,String> r_map = record.toMap();
				String icd10_id = get_sans_quotes(r_map, "'ICD-10-CM CODE'");
				String icd_name = get_sans_quotes(r_map, "'ICD-10-CM CODE DESCRIPTION'");
				String ccs_single = get_sans_quotes(r_map, "'CCS CATEGORY'");
				String ccs_multi = get_sans_quotes(r_map,"'MULTI CCS LVL 2'");
				Node ccs;
				if(ccs_multi.equals(last_ccs_long_id)){
					ccs = last_ccs;
				}else{
					ccs = db.findNode(Labels.CCSdx, "long_id", ccs_multi);
					last_ccs_long_id = ccs_multi;
					last_ccs = ccs;
				}
				Node icd10 = find_icd10(db, icd10_id, icd_name);
				if(! ccs.hasProperty("short_id") ){
					try{
						ccs.setProperty("short_id", Integer.parseInt(ccs_single));
					}catch(NumberFormatException e){
						logger.info("short_id: "+ccs_single);
					}
				}
				//if(! has_rel(ccs, icd10)); // For speed we make sure all ICD10-CCS rels are removed
				icd10.createRelationshipTo(ccs, RelTypes.is_a);
				tx.success();
				tx.close();
			}
		}
	}
	
	@Description( "Add ICD->CCS relations" )
	@PluginTarget( GraphDatabaseService.class )
	public String icd10_to_ccs(
			@Source GraphDatabaseService db,            
			@Description( "Input File" )
			@Parameter( name = "in_file" ) String in
	) throws IOException{       
		//try(Utility u = Utility.graph_util(db)){
		_icd10_to_ccs(db, in);
		return "complete";
		//}
	}
}
