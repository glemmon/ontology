//package org.hci.updb;
package org.eihg.phevor.plugins;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.GraphConvenience.RelTypes;
import org.eihg.phevor.utility.Utility;


import java.util.Map;
import java.util.logging.Logger;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

public class ICDtoCCS extends ServerPlugin
{	
	//private static Logger logger = Logger.getLogger("org.eihg.phevor.plugins"); 

	private static boolean has_rel(Node ccs, Node icd10){
		for( Relationship r : icd10.getRelationships(Direction.OUTGOING, RelTypes.is_a)){
			if( icd10.equals(r.getEndNode() ))
				return true;
		}
		return false;
	}
	
	private static void _icd10_to_ccs(GraphDatabaseService db, String in) throws IOException{
		final CSVFormat format = CSVFormat.DEFAULT.withQuote('\'').withFirstRecordAsHeader();
		try(
				final Reader reader = new FileReader(in);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			String last_ccs_long_id = null;
			Node last_ccs = null;
			for (CSVRecord record : records) {
				//logger.info("record: "+record.toString());
				Map<String,String> r_map = record.toMap();
				//logger.info("r_map: "+r_map.toString());
				String icd10_id = r_map.get("ICD-10-CM CODE");
				String ccs_cat = r_map.get("CCS CATEGORY");
				String ccs_parents = r_map.get("MULTI CCS LVL 2");
				String ccs_long_id = ccs_parents+'.'+ccs_cat;
				//logger.info("icd10_id: "+icd10_id+", ccs_id: "+ccs_long_id);
				Node ccs;
				if(ccs_long_id.equals(last_ccs_long_id)){
					ccs = last_ccs;
				}else{
					ccs = db.findNode(Labels.CCS, "long_id", ccs_long_id);
					last_ccs_long_id = ccs_long_id;
					last_ccs = ccs;
				}
				Node icd10 = db.findNode(Labels.ICD10dx, "id", icd10_id);
				//logger.info("icd10: "+icd10.toString()+", ccs: "+ccs.toString());
				//if(! has_rel(ccs, icd10)); // For speed we make sure all ICD10-CCS rels are removed
				icd10.createRelationshipTo(ccs, RelTypes.is_a);
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
			//logger.info("In: "+in);
			_icd10_to_ccs(db, in);
			return "complete";
		}
	}
}
