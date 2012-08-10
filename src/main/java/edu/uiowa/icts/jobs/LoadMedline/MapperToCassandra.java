package edu.uiowa.icts.jobs.LoadMedline;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperToCassandra extends Mapper<LongWritable, Text, ByteBuffer, List<Mutation>>  {
    private static final Logger logger = LoggerFactory.getLogger(MapperToCassandra.class);

	  private Text raw_xml = new Text();
	  private String pmid = new String();

	  private static enum Count { DOCS };
	  
	    private static Text outputKey= new Text();

	    private static Text outputColumn = new Text();
	    
	  @Override
	  public void setup(Context context) {
		  outputColumn.set(context.getConfiguration().get("output.column.name"));
	  }
	  
	  
	  @Override
	  public void map(LongWritable key, Text doc, Context context) { //throws IOException, InterruptedException {
	  	context.getCounter(Count.DOCS).increment(1);
	  	//raw_xml.set( doc );
	  	String xml = doc.toString();
	  	
	      int start = xml.indexOf("<PMID Version=");

	      if (start == -1) {
	        throw new RuntimeException("Error Getting PMID");
	      } else {
	        int end = xml.indexOf("</PMID>", start);
	        pmid = xml.substring(start + 18, end);
	      }
	      xml = xml.replaceAll("\n","");
	  //    logger.info(pmid );
		try {
			raw_xml.set(xml);
			//context.write(new Text(pmid), raw_xml);
			//logger.info("Writing PMID: " + pmid);
			context.write(ByteBufferUtil.bytes(pmid), Collections.singletonList(getMutation(raw_xml)));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Error Message", e);
			//e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("Error Message", e);
			//e.printStackTrace();
		}
			
		     
	  }
	  

	    private static Mutation getMutation(Text value)
	    {
	        
			Column c = new Column();
	  
			c.setName(Arrays.copyOf(outputColumn .getBytes(), outputColumn.getLength()));
			
			c.setValue(ByteBufferUtil.bytes(value.toString()));
			c.setTimestamp(System.currentTimeMillis());

			Mutation m = new Mutation();
			m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
			m.column_or_supercolumn.setColumn(c);
			return m;
			
	    }

	  
	  
}

