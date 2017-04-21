//package org.hci.updb;
package org.eihg.phevor.plugins;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.eihg.phevor.utility.GraphConvenience.Labels;
import org.eihg.phevor.utility.GraphConvenience.RelTypes;
import org.eihg.phevor.utility.Utility;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class AddParents extends ServerPlugin
{
	private static final Map<String,Label> domain_label = ImmutableMap.of("ICD9CM", Labels.ICD9dx, "ICD-10-CM", Labels.ICD10dx);
	
	private static List<String> parents_from_ccs(String ccs_id){
		List<String> parents = Lists.newArrayList();
		parents.add(ccs_id);
		for(int i=ccs_id.length()-1; i > 0; --i ){
			if(ccs_id.charAt(i)!='.') continue;
			parents.add(ccs_id.substring(0, i));
		}
		return parents;
	}
	
	private static String find_ccs(GraphDatabaseService db, String code, Label label){
		Node n = db.findNode(label, "id", code);
		if(n==null) return "";
		Iterable<Relationship> rels = n.getRelationships(RelTypes.is_a, Direction.OUTGOING);
		List<String> ccs_codes = Lists.newArrayList();
		for( Relationship rel : rels ){
			Node ccs = rel.getEndNode();
			if( ! ccs.hasLabel(label) ) continue;
			List<String> ccs_ids = parents_from_ccs((String) ccs.getProperty("long_id"));
			ccs_codes.addAll(ccs_ids);
		}
		if(ccs_codes.size() < 1) return find_ccs(db, code+"0", label);
		return String.join(",", ccs_codes);
	}
	
	private static String get_ccs(GraphDatabaseService db, String code, String domain){
		Label label = domain_label.get(domain);
		code = code.trim().replace(".", ""); // remove punctuation
		return find_ccs(db, code, label);
	}
		
	private static void _add_parents_to_file(GraphDatabaseService db, String in, String out) throws IOException{
		final CSVFormat format = CSVFormat.TDF.withFirstRecordAsHeader();
		try(
				final Reader reader = new FileReader(in);
		){
			final Iterable<CSVRecord> records = format.parse(reader);
			final String[] new_header = ArrayUtils.add(format.getHeader(), "CCS");
			
			try(		
					final FileWriter writer = new FileWriter(out);
					final CSVPrinter printer = format.withHeader(new_header).print(writer)
			){
				printer.printRecord(Arrays.asList(new_header));
				for (CSVRecord record : records) {
					Map<String,String> r_map = record.toMap();
					String domain = r_map.get("DOMAIN");
					String code = r_map.get("CODE");
					String ccs = get_ccs(db, code, domain);
					Iterable<String> itrb = Iterables.concat(record, Collections.singletonList(ccs));
					printer.printRecord(itrb);
				}
				printer.flush();
		        printer.close();
			}
		}
	}
	
	@Description( "Find ROOT associated with this label and return the tree in 'mtree' format" )
	@PluginTarget( GraphDatabaseService.class )
	public String add_parents_to_file(
			@Source GraphDatabaseService db,            
			@Description( "Input File" )
			@Parameter( name = "in_file" ) String in,
			@Description( "Output File" )
			@Parameter( name = "out_file" ) String out

	) throws IOException{       
		try(Utility u = Utility.graph_util(db)){
			_add_parents_to_file(db, in, out);
			return "complete";
		}
	}
}
