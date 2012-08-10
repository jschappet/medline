package edu.uiowa.icts.jobs.AuthorIndex;

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
import edu.uiowa.icts.util.Defaults;
import edu.uiowa.icts.medline.Citation;
import edu.uiowa.medline.xml.MedlineCitation;

public class AuthorMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, ByteBuffer, List<Mutation>>  {
    private static final Logger logger = LoggerFactory.getLogger(AuthorMapper.class);


	  private Text raw_xml = new Text();
	  private String pmid = new String();
      private ByteBuffer sourceColumn;

	private JAXBContext jContext;

	private Unmarshaller unmarshaller;

	  private static enum Count { DOCS, AUTHORS };
	  
	    

	    private static Text outputColumn = new Text();
	    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
	    
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
		List<String[]> authorList = citation.getAuthorList();
		logger.info("Author Count: " + authorList.size() + " pmid:" + citation.getPmid());
			
		for (String[] author : authorList ) { 
			context.getCounter(Count.AUTHORS).increment(1);
			//inserts.add(getMutation(citation.getPmid(), null));
			ByteBuffer bytes = getBytes(author);
			List<Mutation> authName = new ArrayList<Mutation>();
			StringBuffer displayName = new StringBuffer(author[0]);
			authName.add(getMutation("last_name", author[0] != null ? author[0].toLowerCase() : ""));
			if (author.length >= 1 ) {
				authName.add(getMutation("first_name", author[1] != null ? author[1].toLowerCase() : ""));
				displayName.append(", " + author[1]);
			}
			if (author.length >= 2 ) {
				displayName.append(" [" + author[2] + "]");
				authName.add(getMutation("initial", author[2] != null ? author[2].toLowerCase() : ""));
			}
			authName.add(getMutation("displayName", displayName.toString()));
			try {
				context.write(bytes, authName);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				logger.error("Error inserting Author", e1);
			}
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
	  
	  
	  private ByteBuffer getBytes(String[] author) {
		  String one = null ;
		  String two = null;
		  String three = null;
		  try {
			one = author[0];
			  two = author[1];
			  three = author[2];
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return null;
			
		}
		  return ByteBufferUtil.bytes((one+ Defaults.NAME_SEPERATOR + two + Defaults.NAME_SEPERATOR +three).toLowerCase());
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

