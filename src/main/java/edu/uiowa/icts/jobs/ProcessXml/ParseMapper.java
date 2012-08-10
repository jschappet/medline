package edu.uiowa.icts.jobs.ProcessXml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

public class ParseMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, ByteBuffer, List<Mutation>>  {
    private static final Logger logger = LoggerFactory.getLogger(ParseMapper.class);

	  private Text raw_xml = new Text();
	  private String pmid = new String();
      private ByteBuffer sourceColumn;

	private JAXBContext jContext;

	private Unmarshaller unmarshaller;

	  private static enum Count { DOCS };
	  
	    

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
	  public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException {
	  	context.getCounter(Count.DOCS).increment(1);
    	Citation citation = null;
    	IColumn column = columns.get(sourceColumn);
        if (column == null)
            return;
        String value = ByteBufferUtil.string(column.value());
			try {
				citation = new Citation(value,unmarshaller);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("Error Message", e);
				//e.printStackTrace();
			}
			
			if (citation == null) {
				logger.info("Blank Citation: " + ByteBufferUtil.string(key));
			} else {
				ArrayList<Mutation> inserts = new ArrayList<Mutation>();
				inserts.add(getMutation("journalTitle",citation.getJournalTitle() ));
				
				inserts.add(getMutation("abstractText",citation.getAbstractText() ));
				inserts.add(getMutation("articleTitle",citation.getArticleTitle()));
				if (citation.getPublished() != null) {
					inserts.add(getMutation("publishDate",""+citation.getPublished().getTime()));
					inserts.add(getMutation("publishYearMonth",format.format(citation.getPublished())));
				}
				if (citation.getCreated() != null) {
					inserts.add(getMutation("createdDate",""+citation.getCreated().getTime()));
					inserts.add(getMutation("createdYearMonth",format.format(citation.getCreated())));
				}
				if (citation.getRevised() != null) {
					inserts.add(getMutation("revisedYearMonth",format.format(citation.getRevised())));
				}
				if (citation.getCompleted() != null) {
					inserts.add(getMutation("completedYearMonth",format.format(citation.getCompleted())));
				} 
	 			context.write(key, inserts);
			}
			
		     
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

