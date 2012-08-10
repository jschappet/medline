package edu.uiowa.icts.jobs.MeshTermList;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiowa.icts.medline.Citation;
import edu.uiowa.icts.util.Combinations;

public class MyMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>  {
    private static final Logger logger = LoggerFactory.getLogger(MyMapper.class);

	  private Text raw_xml = new Text();
	  private String pmid = new String();
      private ByteBuffer sourceColumn;

	private JAXBContext jContext;

	private Unmarshaller unmarshaller;

	  private static enum Count { DOCS, TERMS };
	  
	    
	  private static int minCount = 1;
	  private static int maxCount = 1 ;

	    private static Text outputColumn = new Text();
	    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
	   
	
	    
	    
	  @Override
	  public void setup(Context context) {
		  logger.debug("setup");
		//  outputColumn.set(context.getConfiguration().get("output.column.name"));
		  sourceColumn = ByteBufferUtil.bytes((context.getConfiguration().get("source.column.name")));
		  try {
				jContext=JAXBContext.newInstance("edu.uiowa.medline.xml");
				
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				logger.error("Error Message", e);
				//e.printStackTrace();
			} 
			
			try {
				unmarshaller = jContext.createUnmarshaller() ;
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				logger.error("Error Message", e);
				//e.printStackTrace();
			}
			logger.debug("done");
	  }
	  
	  
	  @Override
	  public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) {
	        logger.info("mapping");

	  	context.getCounter(Count.DOCS).increment(1);
    	Citation citation = null;
    	IColumn column = columns.get(sourceColumn);
        if (column == null)
            return;
        String value = null;
		try {
			value = ByteBufferUtil.string(column.value());
		} catch (CharacterCodingException e) {
			// TODO Auto-generated catch block
			logger.error("Error Getting Value", e);
		}
		citation = new Citation(value, unmarshaller);
		List<String> termList = citation.getMajorMeshHeading();

		String pmid = citation.getPmid();
		logger.info("pmid:" + pmid);
		
		for (String term : termList) {
			try {
			  	context.getCounter(Count.TERMS).increment(1);

				context.write(new Text(term),new Text(pmid));
			} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error("Error writing term", e);
			}
			 
			
		}
			
			
		     
	  }
	    
	    
}

