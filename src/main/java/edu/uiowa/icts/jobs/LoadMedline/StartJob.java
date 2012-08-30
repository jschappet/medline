/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.uiowa.icts.jobs.LoadMedline;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.uiowa.icts.util.ReducerToFilesystem;
import edu.uiowa.icts.util.XmlInputFormat;

/**
 * <p>
 * Simple demo program that counts all the documents in a collection of MEDLINE citations. This
 * provides a skeleton for MapReduce programs to process the collection. The program takes three
 * command-line arguments:
 * </p>
 *
 * <ul>
 * <li>[input] path to the document collection
 * <li>[output-dir] path to the output directory
 * <li>[mappings-file] path to the docno mappings file
 * </ul>
 *
 * <p>
 * Here's a sample invocation:
 * </p>
 *
 * <blockquote><pre>
 * setenv HADOOP_CLASSPATH "/foo/cloud9-x.y.z.jar:/foo/guava-r09.jar"
 *
 * hadoop jar cloud9-x.y.z.jar edu.umd.cloud9.collection.trec.DemoCountTrecDocuments2 \
 *   -libjars=guava-r09.jar \
 *   /shared/collections/trec/trec4-5_noCRFR.xml \
 *   /user/jimmylin/count-tmp \
 *   /user/jimmylin/docno-mapping.dat
 * </pre></blockquote>
 *
 * @author James Schappet
 */
public class StartJob extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StartJob.class);
  //private static enum Count { DOCS };


  static final String KEYSPACE = "MEDLINE";
  static final String COLUMN_FAMILY = "MedlineCitation";

  static final String OUTPUT_REDUCER_VAR = "output_reducer";
  static final String OUTPUT_DEFAULT = "cassandra"; // or "filesystem"
  static final String OUTPUT_COLUMN_FAMILY = "output_words";
  private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";

  private static final String CONF_COLUMN_NAME = "columnname";
  
  



  /**
   * Creates an instance of this tool.
   */
  public StartJob() {
  }


  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
	   OptionParser parser = new OptionParser( "?i::x::o::t::" );

       OptionSet options = parser.parse(args);

       if (options.has("?")) {
    	   printUsage();
       }

       String inputPath = "medline";
       if (options.has("i")) {
    	   inputPath = (String)options.valueOf("i");
       }

       String outputPath = "MedlineCitation";
       if (options.has("o")) {
    	   outputPath = (String)options.valueOf("o");
       }
       
       String outputTarget = "cassandra";
       if (options.has("t")) {
    	   outputTarget = (String)options.valueOf("t");
       }
       
           
    LOG.info("Tool: " + StartJob.class.getCanonicalName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output dir: " + outputPath);
    LOG.info(" - output Target: " + outputTarget);
 //   LOG.info(" - docno mapping file: " + mappingFile);

    Job job = new Job(getConf(), StartJob.class.getSimpleName());
    job.setJarByClass(StartJob.class);
    job.setNumReduceTasks(0);


    FileInputFormat.setInputPaths(job, new Path(inputPath));
    
    job.getConfiguration().set("xmlinput.start", "<MedlineCitation O");
    job.getConfiguration().set("xmlinput.end", "</MedlineCitation>");
    
    job.getConfiguration().set("output.column.name","RAW_XML");
    
    job.setInputFormatClass(XmlInputFormat.class);
    


    if ("cassandra".equalsIgnoreCase(outputTarget)) {
    	ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
    	ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, outputPath);
        
        job.setMapperClass(MapperToCassandra.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
    	LOG.info("Writing output to Cassandra");
    	//job.setReducerClass(ReducerToCassandra.class);
    	job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
        
        ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
        //org.apache.cassandra.dht.LocalPartitioner
        ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");

    } else {
    	LOG.info("Writing output to FileSystem");
    	FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);

        job.setReducerClass(ReducerToFilesystem.class);
    
        
        // Delete the output directory if it exists already.
        FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);

    }
    try {
		job.waitForCompletion(true);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		LOG.error("Createing Job", e);
	}

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new StartJob(), args);
  }
  
  
	 static private void printUsage() {
	    	System.out.println("\n");
	    	System.out.println("Load Medline Citations from XML\n");
	    	System.out.println("Usage: hadoop jar medline.jar [-?]");
	    	System.out.println("\t-? -- print this message");
	    	System.out.println("\t-t [cassandra|filesystem] ");
	    	System.out.println("\t-o [ColFamily or Directory] ");
	    	System.out.println("\t-i [Directory] ");
	    	
	    	System.out.println("");
	    	System.exit(1);
	    }
}
