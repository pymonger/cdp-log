package gov.nasa.jpl.cdp.log;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gov.nasa.jpl.cdp.provenance.Provo;


/**
 */
public class log_parser
{
	final java.lang.String comment_line = "##";
    final int comment_line_length = comment_line.length();
    final java.lang.String opm_used = "processUsedArtifacts";
    final java.lang.String opm_proc_controlled_by = "processStartedByAgent";
    final java.lang.String opm_proc_triggered_proc = "processTriggeredNewProcess";
    final java.lang.String opm_proc_generated_art = "processGeneratedArtifacts";
    final java.lang.String opm_sess_generated_art = "sessionGeneratedArtifacts";
    final java.lang.String opm_sess_used_art = "sessionUsedArtifacts";
    final java.lang.String opm_proc_start = "processStart";
    final java.lang.String opm_proc_end = "processEnd";
    Provo proves_int;
    
    // params for Virtuoso backend
    java.lang.String virtUrl;
    java.lang.String virtUsername;
    java.lang.String virtPassword;
    
    // params for TDB
    java.lang.String tdbDir;
    
    // params for in-memory model
    com.hp.hpl.jena.rdf.model.Model ds;
    
    // which backend to use
    java.lang.String backend;
    
    // pattern to match all spaces
    Pattern emptyPtn = Pattern.compile("\\s*");
    
    // runtime environment
    java.lang.String runtimeEnv = null;
    
    // construct Virtuoso-backed model
    public log_parser(java.lang.String url, java.lang.String username, 
    		java.lang.String password) {
    	virtUrl = url;
    	virtUsername = username;
    	virtPassword = password;
    	backend = "virtuoso";
    }
    
    // construct TDB-backed model
    public log_parser(java.lang.String dir) {
    	tdbDir = dir;
    	backend = "tdb";
    }
    
    // construct in-memory model
    public log_parser(com.hp.hpl.jena.rdf.model.Model md) {
    	ds = md;
    	backend = "memory";
    }
    
    // close model and dataset effectively writing model to the store
    public void close() {
        if (proves_int != null) proves_int.close();
    }

    // parse the log file sent from the client
    public java.lang.String parse_log(BufferedReader br) {
        java.lang.String session = null; // session URI for this session
        java.lang.String stime = null;  // start time of parent process
        java.lang.String etime = null;  // end time of parent process
        java.lang.String parent_ps = null;  // the parent process of all
        java.lang.String agent = null;  // the agent who started parent process

        java.lang.String thisLine = null;
        
        // hashtable to store the start and end times for processes
        Hashtable proc_time_hash = new Hashtable();

        try {
          // Open an input stream
          // BufferedReader br = new BufferedReader(new FileReader(log_file));


          int line_num = 1;
          while ((thisLine = br.readLine()) != null) { // read through log lines
            //System.out.println("\n\n------- Line number: " + line_num + ":  " + thisLine);

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
                //System.out.println("comment line: " + thisLine);
            	if (line_num == 1) {
            		if (backend.contentEquals("memory")) {
	            		proves_int = new Provo(ds, thisLine.split("#")[2]);
            		}else if (backend.contentEquals("virtuoso")) {
            			proves_int = new Provo(virtUrl, virtUsername,
            					virtPassword, thisLine.split("#")[2]);
            		}else if (backend.contentEquals("tdb")) {
            			proves_int = new Provo(tdbDir, thisLine.split("#")[2]);
            		}else {
            			throw new RuntimeException("Unknown backend: " + backend);
            		}
            		session = proves_int.getSessionName();
            	}
                line_num++;
                continue;
            }

            // split the line at #
            java.lang.String[] out1 = thisLine.split("#");

            //System.out.println("----------- split the line at #:  -------------");
            // test print
            for(int i=0; i<out1.length; i++) {
              //System.out.print("======   ");
              //System.out.println(out1[i]);
            }
            //System.out.println("-------------------");
            //System.out.println("");

            //System.out.println("out1.length: " + out1.length);
            // skip a line that does not contain "predicate"
            if (out1.length < 4) {
              line_num++;
              continue;
            }

            // out1[3] is "predicate"
            String predicate = out1[3];
            predicate = predicate.replaceAll(" ", "");
            //System.out.println("predicate: " + predicate);

            if (predicate.equals(opm_used)) {
              //System.out.println("\n-------- in section: " + opm_used);

              // out1[4] is a process and out1[6] is entity(s)
              java.lang.String ps = out1[4];
              java.lang.String as = out1[6];
              java.lang.String role = out1[7];
              //System.out.println("ps: "+ps+", as: "+as);

              // split as (comma split list)
              java.lang.String[] arts = as.split(",");
              
              // get role
              java.lang.String[] roleCmps = role.split("/");

              for(int j=0; j<arts.length; j++) {
                java.lang.String aart = arts[j].trim();
                System.out.println("aart: "+aart);
                java.lang.String[] aarts = aart.split("::");
                if (aarts.length == 2)
                	proves_int.used(ps, aarts[0], roleCmps[1], aarts[1]);
                else
                	proves_int.used(ps, aarts[0], roleCmps[1]);
              }

            }
            else if (predicate.equals(opm_proc_start)) {
              //System.out.println("\n-------- in section: " + opm_proc_start);

              // out1[4] is a process and out1[6] is entity(s)
              java.lang.String ps = out1[4];
              java.lang.String sf = out1[5];
              java.lang.String version = out1[6];
              java.lang.String args = "";
              if (out1.length == 8) args = out1[7];
              //System.out.println("ps: "+ps+", sf: "+sf);
              //System.out.println("args: '" + args + "'");
              
              Matcher matcher = emptyPtn.matcher(args);
              boolean found = matcher.find();
              if (found) proves_int.getProcess(ps, sf);
              else proves_int.getProcess(ps, sf, args);
            }
            else if (predicate.equals(opm_proc_controlled_by)) {
              //System.out.println("\n-------- in section: " + opm_proc_controlled_by);
              //System.out.println("-------- calling wasControlledBy() with only start time");
              // out1[4] is a process and out1[5] is an agent
              java.lang.String ps = out1[4];
              parent_ps = new java.lang.String(ps);
              java.lang.String ags = out1[5];
              agent = new java.lang.String(ags);

              // out1[1] is time stamp (startTime)
              stime = out1[1];

              //System.out.println("stime: "+stime);
              //System.out.println("ps: "+ps+", ags: "+ags);

              proves_int.wasControlledBy(ps, ags, stime);
              
              // add agent to runtime environment
              if (runtimeEnv != null)
            	  proves_int.addRuntimeEnvironment(runtimeEnv, agent);

            }
            else if (predicate.equals(opm_proc_triggered_proc)) {
              //System.out.println("\n-------- in section: " + opm_proc_triggered_proc);
              // out1[4] is a process and out1[5] is a new process
              java.lang.String ps = out1[4];
              java.lang.String ps2 = out1[5];

              //System.out.println("ps: "+ps+", ps2: "+ps2);

              proves_int.wasTriggeredBy(ps2, ps);
              
              /* add WasControlledBy for process */
              // out1[1] is time stamp (startTime)
              proc_time_hash.put(ps2, out1[1]); // save start time for procEnd call
              proves_int.wasControlledBy(ps2, agent, out1[1]);

            }
            else if (predicate.equals(opm_proc_generated_art)) {
              //System.out.println("\n-------- in section: " + opm_proc_generated_art);
              // out1[4] is a process and out1[6] is entitys
              java.lang.String ps = out1[4];
              java.lang.String as = out1[6];
              java.lang.String role = out1[7];
              System.out.println("ps: "+ps+", as: "+as);
              System.out.println("role: "+role);

              // split as (comma split list)
              java.lang.String[] arts = as.split(",");
              
              // get role
              java.lang.String[] roleCmps = role.split("/");

              for(int j=0; j<arts.length; j++) {
                java.lang.String aart = arts[j].trim();
                System.out.println("aart: "+aart);
                java.lang.String[] aarts = aart.split("::");
                if (aarts.length == 2)
                	proves_int.wasGeneratedBy(aarts[0], ps, roleCmps[1], aarts[1]);
                else
                	proves_int.wasGeneratedBy(aarts[0], ps, roleCmps[1]);
              }

            }
            else if (predicate.equals(opm_sess_generated_art)) {
              //System.out.println("\n-------- in section: " + opm_sess_generated_art);
              // out1[4] is a session and out1[6] is entitys
              java.lang.String se = out1[4];
              java.lang.String as = out1[6];
              java.lang.String role = out1[7];
              System.out.println("se: "+se+", as: "+as);
              System.out.println("role: "+role);

              // split as (comma split list)
              java.lang.String[] arts = as.split(",");
              
              // get role
              java.lang.String[] roleCmps = role.split("/");
              
              // add runtime environment to session
              runtimeEnv = roleCmps[1];
              if (agent == null) proves_int.addRuntimeEnvironment(runtimeEnv);
              else proves_int.addRuntimeEnvironment(runtimeEnv, agent);

              for(int j=0; j<arts.length; j++) {
                java.lang.String aart = arts[j].trim();
                System.out.println("aart: "+aart);
                java.lang.String[] aarts = aart.split("::");
                if (aarts.length == 2)
                	proves_int.addSessionGeneratedFile(aarts[0], runtimeEnv, aarts[1]);
                else
                	proves_int.addSessionGeneratedFile(aarts[0], runtimeEnv);
                
              }
            }
            else if (predicate.equals(opm_sess_used_art)) {
	          //System.out.println("\n-------- in section: " + opm_sess_generated_art);
	          // out1[4] is a session and out1[6] is entitys
	          java.lang.String se = out1[4];
	          java.lang.String as = out1[6];
	          java.lang.String role = out1[7];
	          System.out.println("se: "+se+", as: "+as);
	          System.out.println("role: "+role);
	
	          // split as (comma split list)
	          java.lang.String[] arts = as.split(",");
	            
	          // get role
	          java.lang.String[] roleCmps = role.split("/");
	          
	          // add runtime environment to session
	          if (agent == null) proves_int.addRuntimeEnvironment(roleCmps[1]);
              else proves_int.addRuntimeEnvironment(roleCmps[1], agent);
	
	          for(int j=0; j<arts.length; j++) {
	            java.lang.String aart = arts[j].trim();
	            System.out.println("aart: "+aart);
	            java.lang.String[] aarts = aart.split("::");
	            if (aarts.length == 2)
	              proves_int.addSessionUsedFile(aarts[0], roleCmps[1], aarts[1]);
	            else
	              proves_int.addSessionUsedFile(aarts[0], roleCmps[1]);
	              
	          }
	        }
            else if (predicate.equals(opm_proc_end)) {
              java.lang.String ps = out1[4];
              if (ps.equals(parent_ps)) {
                //System.out.println("\n-------- in section: " + opm_proc_end);
                //System.out.println("-------- calling wasControlledBy() with both start and end time");
                etime = out1[1];
                //System.out.println("stime: "+stime);
                //System.out.println("etime: "+etime);
                //System.out.println("ps: "+ps+", agent: "+agent);
                proves_int.wasControlledBy(parent_ps, agent, stime, etime);
              }else {
              	/* add WasControlledBy for process - end time */
                  // out1[1] is time stamp (endTime)
                  proves_int.wasControlledBy(ps, agent, (String) proc_time_hash.get(ps), out1[1]);
              }

            }
            else {
              //System.out.println("----- line " + line_num + " cannot be parsed at this moment.");
              //System.out.println(thisLine);
            }

            line_num ++;
          } // end while

          // System.out.println("------------- out here: ");
          //proves_int.write(System.out, "TURTLE");
        }
        // Catches any error conditions
        catch (IOException e) {
          System.err.println ("Unable to read from the BufferedReader: " + br);
          // System.exit(-1);
        }

        if (proves_int == null) {
        	throw new RuntimeException("Failed to parse any provenance " +
        			"log lines. Check it's content or format.");
        }
        
        return session;
    }
}

