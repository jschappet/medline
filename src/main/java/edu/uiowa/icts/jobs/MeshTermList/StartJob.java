/******
 * StartJob
 * 
 * This App will process the Raw Xml from Medline Citation into K,V pairs to be loaded into Cassandra
 * 
 * Usage:
 * 	hadoop jar medline.jar edu.uiowa.icts.hadoop.ProcessRawXml [ options ]
 * 
 * 
 */
package edu.uiowa.icts.jobs.MeshTermList;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class StartJob extends Configured implements Tool {
	  private static final Logger LOG = Logger.getLogger(StartJob.class);

	  private static final String IN_COLUMN_NAME = "source.column.name";
	  
	  private static final String KEYSPACE = "MEDLINE";
	  private static final String COLUMN_FAMILY = "MedlineCitation";

	  
	/**
	 * @param args
	 */
	public static void main(String[] args)  {
	    try {
			ToolRunner.run(new Configuration(), new StartJob(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Error Message", e);
			//e.printStackTrace();
		}

	}
	
	 static private void printUsage() {
	    	System.out.println("\n");
	    	System.out.println("Build Mesh Term List\n");
	    	System.out.println("Usage: hadoop jar medline.jar [-?]");
	    	System.out.println("\t-? -- print this message");
	    	System.out.println("");
	    	System.exit(1);
	    }
	    

	

    public int run(String[] args) throws Exception
    {
    	
 	   OptionParser parser = new OptionParser( "?i::x::o::" );

       OptionSet options = parser.parse(args);

       if (options.has("?")) {
    	   printUsage();
       }


       String outPutColumnName = "Mesh";
       if (options.has("o")) {
    	   outPutColumnName = (String)options.valueOf("o");
       }
    	String columnName = "RAW_XML";
       
       
       
       LOG.info("Starting Job");
		try {

			getConf().set(IN_COLUMN_NAME, columnName );
		
			
			Job job = new Job(getConf(), "Generate Mesh Term List");
            job.setInputFormatClass(ColumnFamilyInputFormat.class);
		    job.setNumReduceTasks(1);

            job.setJarByClass(StartJob.class);
            job.setMapperClass(MyMapper.class);

            job.setInputFormatClass(ColumnFamilyInputFormat.class);
            job.setMapOutputKeyClass(Text.class);  //check 
	    	job.setMapOutputValueClass(Text.class); //check
	    	
	    	
	    	
	    	job.setReducerClass(MyReducer.class); // check
	    	job.setOutputFormatClass(ColumnFamilyOutputFormat.class);  // check
	    	job.setOutputKeyClass(ByteBuffer.class); // check
	    	job.setOutputValueClass(List.class); // check
	    	
	    	
	    	
//	    	
//	    	
//            ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, outPutColumnName);
//            ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
//            //org.apache.cassandra.dht.LocalPartitioner
//	        ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
//	        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
//	        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
//	        
//	        
//	        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
//	        
//	        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

	    	
            ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, outPutColumnName);
            job.setInputFormatClass(ColumnFamilyInputFormat.class);
            ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
            //org.apache.cassandra.dht.LocalPartitioner
	        ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
	        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
	        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
	        
	        
	        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
//	        SliceRange slice_range = new SliceRange();
//	        slice_range.setStart(ByteBufferUtil.bytes(startPoint));
//	        slice_range.setFinish(ByteBufferUtil.bytes(endPoint));
//	        
//	        predicate.setSlice_range(slice_range);
	        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

          	job.waitForCompletion(true);
	    	
	    	
          	job.waitForCompletion(true);

			
			
			
		} catch (Exception e) {
			LOG.error("Exception: " , e);
		}
		LOG.info("Done Job");
		return 0;
    }
}
