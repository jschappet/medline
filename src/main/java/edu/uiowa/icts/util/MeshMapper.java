package edu.uiowa.icts.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
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
import org.xml.sax.InputSource;

import edu.uiowa.icts.medline.Citation;
import edu.uiowa.medline.xml.MedlineCitation;

public class MeshMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, ByteBuffer, List<Mutation>>  {
    private static final Logger logger = LoggerFactory.getLogger(MeshMapper.class);

	  private ByteBuffer sourceColumn;

	private JAXBContext jContext;

	private Unmarshaller unmarshaller;

	  private static enum Count { DOCS, TERMS };
	  
	    

	  @Override
	  public void setup(Context context) {
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
	  }
	  
	  
	  @Override
	  public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) {
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
			e.printStackTrace();
		}
        logger.info("mapping");
		citation = new Citation(value, unmarshaller);
		List<String> termList = citation.getMajorMeshHeading();
			
		for (String term : termList ) { 
			context.getCounter(Count.TERMS).increment(1);
		
			ByteBuffer bytes = getBytes(term);
			
			if (bytes != null) {
				try {
					context.write(bytes, Collections.singletonList(getMutation(citation.getPmid(), "1")));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error("Error writing Context",e);
				} 
			}
		} 
 			
			
			
		     
	  }
	  
	  
	  private ByteBuffer getBytes(String term) {
		  
		  return ByteBufferUtil.bytes(term.toLowerCase());
	  }

	    private static Mutation getMutation(String colName, String value)
	    {
	        
			Column c = new Column();
	  
			c.setName(ByteBufferUtil.bytes(colName));
			
			c.setValue(ByteBufferUtil.bytes(value));
			c.setTimestamp(System.currentTimeMillis());

			Mutation m = new Mutation();
			m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
			m.column_or_supercolumn.setColumn(c);
			return m;
			
	    }

	  
	  
}

