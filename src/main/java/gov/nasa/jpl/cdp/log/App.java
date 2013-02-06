package gov.nasa.jpl.cdp.log;

import java.io.*;
import java.lang.*;
import java.util.*;

import gov.nasa.jpl.cdp.provenance.Opmo;

import org.ontoware.aifbcommons.collection.ClosableIterator;
import org.ontoware.rdf2go.Reasoning;
import org.ontoware.rdf2go.model.Model;
import org.ontoware.rdf2go.model.Statement;
import org.ontoware.rdf2go.model.Syntax;
import org.ontoware.rdf2go.model.node.BlankNode;
import org.ontoware.rdf2go.model.node.Resource;
import org.ontoware.rdf2go.model.node.URI;
import org.ontoware.rdf2go.model.node.Variable;
import org.ontoware.rdf2go.vocabulary.RDF;
import org.ontoware.rdf2go.vocabulary.RDFS;
import org.ontoware.rdf2go.exception.ModelRuntimeException;
import org.ontoware.rdf2go.impl.jena26.DumpUtils;
import org.ontoware.rdf2go.impl.jena26.ModelImplJena26;


/**
 */
public class App 
{
	public static String version = 
		App.class.getPackage().getImplementationVersion();

	public static void printCommandHelp()
	{
		System.err.println("usage: cdp_log_upload <logfile>" +
				" <Virtuoso JDBC endpoint> <username> <password>");
		System.err.println("e.g. cdp_log_upload provenance_post.log " +
				"jdbc:virtuoso://localhost:1111/charset=UTF-8/log_enable=2 " +
				"dba dba");
		System.err.println("version: " + version);
		System.exit(1);
	}
	
	public static void main( java.lang.String[] args )
    {
    	// make sure there is at least one option/argument
    	if (args.length != 4) printCommandHelp();

        String log_file_name = args[0];
        String url = args[1];
        String username = args[2];
        String password = args[3];
        //String log_file_name = "/home/pan/generateMatchupIndices_provenance.log";
        // log_file_name = "/home/pan/test_log.txt";
        // log_file_name = "/home/pan/example_opm.log";

        try {
          BufferedReader br = new BufferedReader(new FileReader(log_file_name));
          log_parser lp = new log_parser(url, username, password);
          String account = lp.parse_log(br);
          System.out.println(account);
        } catch (FileNotFoundException e) {
          System.err.println ("Unable to read from file: " + log_file_name);
        }
    }


    // parse the log file sent from the client
/*
    public static void parse_log(java.lang.String log_file) {
      // final java.lang.String comment_line = "-----";
      final java.lang.String comment_line = "##";
      final int comment_line_length = comment_line.length();
      final java.lang.String opm_used = "processUsedArtifacts";
      final java.lang.String opm_proc_controlled_by = "processStartedByAgent";
      final java.lang.String opm_proc_triggered_proc = "processTriggeredNewProcess";
      final java.lang.String opm_proc_generated_art = "processGeneratedArtifacts";

      Opm opm_int = new Opm("/data1/packages/data/cdp_TDB_store");

      java.lang.String thisLine = null;

      try {
        // Open an input stream
        BufferedReader br = new BufferedReader(new FileReader(log_file));

	int line_num = 1;
        while ((thisLine = br.readLine()) != null) { // read through log lines
          System.out.println("\n\n------- Line number: " + line_num + ":  " + thisLine);

          // skip lines that are less than 5-char long
          if (thisLine.length() < comment_line_length) {
            line_num++;
            continue;
          }

          // skip comment lines
          // java.lang.String ff = thisLine.substring(0,5);
          java.lang.String ff = thisLine.substring(0, comment_line_length);
          // System.out.println("****** ff: " + ff);
          if(ff.equals(comment_line)) {
            System.out.println("comment line: " + thisLine);
            line_num++;
            continue;
          }

          // split the line at #
          java.lang.String[] out1 = thisLine.split("#");

          System.out.println("----------- split the line at #:  -------------");
          // test print
          for(int i=0; i<out1.length; i++) {
            System.out.print("======   ");
            System.out.println(out1[i]);
          }
          System.out.println("-------------------");
          System.out.println("");

          System.out.println("out1.length: " + out1.length);
          // skip a line that does not contain "predicate"
          if (out1.length < 4) {
            line_num++;
            continue;
          }

          // out1[3] is "predicate"
          String predicate = out1[3];
          predicate = predicate.replaceAll(" ", "");
          System.out.println("predicate: " + predicate);

          if (predicate.equals(opm_used)) {
            System.out.println("\n-------- in section: " + opm_used);

            // out1[4] is a process and out1[6] is an artifact
            java.lang.String ps = out1[4];
            java.lang.String as = out1[6];
            System.out.println("ps: "+ps+", as: "+as);

            opm_int.used(ps, as);
          }
          else if (predicate.equals(opm_proc_controlled_by)) {
            System.out.println("\n-------- in section: " + opm_proc_controlled_by);
            // out1[4] is a process and out1[5] is an agent
            java.lang.String ps = out1[4];
            java.lang.String ags = out1[5];

            System.out.println("ps: "+ps+", ags: "+ags);

            opm_int.wasControlledBy(ps, ags);
          }
          else if (predicate.equals(opm_proc_triggered_proc)) {
            System.out.println("\n-------- in section: " + opm_proc_triggered_proc);
            // out1[4] is a process and out1[5] is a new process
            java.lang.String ps = out1[4];
            java.lang.String ps2 = out1[5];

            System.out.println("ps: "+ps+", ps2: "+ps2);

            opm_int.wasTriggeredBy(ps2, ps);
          }
          else if (predicate.equals(opm_proc_generated_art)) {
            System.out.println("\n-------- in section: " + opm_proc_generated_art);
            // out1[4] is a process and out1[6] is an artifact
            java.lang.String ps = "process#";
            ps = ps.concat(out1[4]);
            java.lang.String as = "artifact#";
            as = as.concat(out1[6]);

            System.out.println("ps: "+ps+", as: "+as);

            opm_int.wasGeneratedBy(as, ps);
          }
          else {
            System.out.println("----- line " + line_num + " cannot be parsed at this moment.");
            System.out.println(thisLine);
          }

          line_num ++;
        } // end while

        // System.out.println("------------- out here: ");
        opm_int.write(System.out, "TURTLE");
      }
      // Catches any error conditions
      catch (IOException e) {
        System.err.println ("Unable to read from file: " + log_file);
        System.exit(-1);
      }

    }
*/



	
    public static void test( java.lang.String[] args )
    {
        System.out.println( "Hello World!" );
        
        //get jena model
        Model model = new ModelImplJena26(Reasoning.rdfs);
        model.open();
        
        /*
		 * before we can do anything useful, we have to define the uris we want
		 * to use
		 */
		// use the uris defined by foaf
		URI foafName = model.createURI("http://xmlns.com/foaf/0.1/name");
		URI foafPerson = model.createURI("http://xmlns.com/foaf/0.1/Person");
		URI foafTitle = model.createURI("http://xmlns.com/foaf/0.1/title");
		URI foafKnows = model.createURI("http://xmlns.com/foaf/0.1/knows");
		URI foafHomepage = model.createURI("http://xmlns.com/foaf/0.1/homepage");
		// use a blank node for the person
		BlankNode werner = model.createBlankNode();
		
		/*
		 * now we can add statements to the model (for easier reading we
		 * replaced the blank nodes cryptical letters with a human readable
		 * version - you will see something different when exectuing this
		 * example
		 */
		// _:blankNodeWerner
		// <http://xmlns.com/foaf/0.1/homepage>
		// <http://www.blue-agents.com> .
		model.addStatement(werner, foafHomepage, model.createURI("http://www.blue-agents.com"));
		// _:blankNodeWerner
		// <http://xmlns.com/foaf/0.1/title>
		// "Mr" .
		model.addStatement(werner, foafTitle, "Mr");
		// _:blankNodeWerner
		// <http://xmlns.com/foaf/0.1/name>
		// "Werner Thiemann" .
		model.addStatement(werner, foafName, "Werner Thiemann");
		
		// _:blankNodeWerner
		// <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
		// <http://xmlns.com/foaf/0.1/Person> .
		model.addStatement(werner, RDF.type, foafPerson);
		
		BlankNode max = model.createBlankNode();
		// _:blankNodeMax
		// <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
		// <http://xmlns.com/foaf/0.1/Person> .
		model.addStatement(max, RDF.type, foafPerson);
		// _:blankNodeMax
		// <http://xmlns.com/foaf/0.1/name>
		// "Max Voelkel" .
		model.addStatement(max, foafName, "Max V?lkel");
		// _:blankNodeMax
		// <http://www.w3.org/2000/01/rdf-schema#seeAlso>
		// <http://www.aifb.uni-karlsruhe.de/WBS/mvo/foaf.rdf.xml> .
		model.addStatement(max, RDFS.seeAlso, model.createURI("http://www.xam.de/foaf.rdf.xml"));
		
		// link via foaf:knows
		// _:blankNodeWerner
		// <http://xmlns.com/foaf/0.1/knows>
		// _:blankNodeMax.
		model.addStatement(werner, foafKnows, max);
		
		// default dump
		DumpUtils.dump(model, null);
		
		// queries
		// get all persons
		
		// ClosableIterator<? extends Statement> it = model.findStatements(Variable.ANY, RDF.type,
		   //     foafPerson);
		// while(it.hasNext()) {
			// Resource person = it.next().getSubject();
			// System.out.println(person + " is a person");
			
			// get foaf:name
			// ClosableIterator<? extends Statement> it2 = model.findStatements(person, foafName,
			     //    Variable.ANY);
			// while(it2.hasNext()) {
				// System.out.println(person + " has the foaf:name " + it2.next().getObject());
			// }
		// }

    }


}

