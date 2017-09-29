//package org.hci.updb;
package org.eihg.phevor.plugins;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.GraphConvenience.RelTypes;
import org.eihg.phevor.utility.Utility;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

public class ICDtoCCS extends ServerPlugin
{	
	private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 

	private static boolean has_rel(Node ccs, Node icd10){
		for( Relationship r : icd10.getRelationships(Direction.OUTGOING, RelTypes.is_a)){
			if( icd10.equals(r.getEndNode() ))
				return true;
		}
		return false;
	}

	private static Node find_icd10(GraphDatabaseService db, String icd10_id, String name){
		String corrected_id;
		String section_id=null;
		if( icd10_id.length() > 3){
			char[] icd10_chrs = icd10_id.toCharArray();
			int index = 3;
			section_id = new String(icd10_chrs, 0, index);
			String diag = new String(icd10_chrs, index, icd10_chrs.length - index);
			corrected_id = section_id+'.'+diag;
		}else corrected_id = icd10_id;
		
		ResourceIterator<Node> node_itr = db.findNodes(Labels.ICD10dx, "id", corrected_id);
		List<Node> nodes = IteratorUtils.toList(node_itr);
		node_itr.close();
		if(nodes.size()==1) return nodes.get(0);
		for(Node n : nodes) 	if(n.hasLabel(Labels.Diagnosis)) return n;
		for(Node n : nodes) 	if(n.hasLabel(Labels.Section)) return n;
		for(Node n : nodes) 	if(n.hasLabel(Labels.Chapter)) return n;
		if( icd10_id.length() <= 3 ) return null;// Can't create chapters and sections here
		Node n = db.createNode(Labels.ICD10dx, Labels.Diagnosis);
		n.setProperty("id", corrected_id);
		n.setProperty("name", name);
		Node section = db.findNode(Labels.ICD10dx, "id", section_id);
		n.createRelationshipTo(section, RelTypes.is_a);
		return n;
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
				logger.info("record: "+record.toString());
				Map<String,String> r_map = record.toMap();
				logger.info("r_map: "+r_map.toString());
				String icd10_id = r_map.get("'ICD-10-CM CODE'");
				icd10_id = icd10_id.replaceAll("'","");
				String icd_name = r_map.get("'ICD-10-CM CODE DESCRIPTION'");
				icd_name = icd_name.replaceAll("'","");
				String ccs_single = r_map.get("'CCS CATEGORY'");
				ccs_single = ccs_single.replaceAll("'","");
				String ccs_multi = r_map.get("'MULTI CCS LVL 2'");
				ccs_multi = ccs_multi.replaceAll("'","");
				Node ccs;
				if(ccs_multi.equals(last_ccs_long_id)){
					ccs = last_ccs;
				}else{
					ccs = db.findNode(Labels.CCSdx, "long_id", ccs_multi);
					last_ccs_long_id = ccs_multi;
					last_ccs = ccs;
				}
				Node icd10 = find_icd10(db, icd10_id, icd_name);
				if(ccs==null) logger.info("ccs is null");
				else logger.info("ccs: "+ccs.toString());
				if(icd10==null) logger.info("icd10 is null");
				else logger.info("icd10: "+icd10.toString());
				logger.info("short_id: "+ccs_single);
				if(! ccs.hasProperty("short_id") ){
					try{
						ccs.setProperty("short_id", Integer.parseInt(ccs_single));
					}catch(NumberFormatException e){
						logger.info("short_id: "+ccs_single);
					}
				}
				//if(! has_rel(ccs, icd10)); // For speed we make sure all ICD10-CCS rels are removed
				Relationship r = icd10.createRelationshipTo(ccs, RelTypes.is_a);
				logger.info("R: "+r.toString());
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
		try(Utility u = Utility.graph_util(db)){
			logger.info("In: "+in);
			_icd10_to_ccs(db, in);
			return "complete";
		}
	}
}
