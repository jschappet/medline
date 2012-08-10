package edu.uiowa.icts.meshcombo;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.SimpleDateFormat;
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

public class ComboMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, ByteBuffer, List<Mutation>>  {
    private static final Logger logger = LoggerFactory.getLogger(ComboMapper.class);

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
		//  outputColumn.set(context.getConfiguration().get("output.column.name"));
		  minCount = Integer.parseInt(context.getConfiguration().get("min.combo.count"));
		  maxCount = Integer.parseInt(context.getConfiguration().get("max.combo.count"));
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
		//logger.info("Mesh Count: " + termList.size() + " pmid:" + citation.getPmid());
		String pmid = citation.getPmid();
		logger.info("pmid:" + pmid);
		Combinations c = new Combinations();
		
		List<String> combos = c.getCombinations(termList, minCount, maxCount);
		//List<Mutation> inserts = new ArrayList<Mutation>();
		logger.info("Combo Count: " + combos.size() + " pmid:" + pmid);
		
		for (String termSet : combos) {
			ByteBuffer bytes = ByteBufferUtil.bytes(termSet);
			String[] terms =  termSet.split("\\|");
			int count = 1;
			for (String term : terms) {
				
				try {
					context.write(bytes, Collections.singletonList(getMutation("term_" + count++,term)));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error("Error writing term", e);
				}
			}
			try {
				Date pubDate = citation.getPublished();
				String pubDateStr = "1";
				if (pubDate != null) {
					pubDateStr = format.format(pubDate);
				}
				context.write(bytes, Collections.singletonList(getMutation(pmid,pubDateStr)));
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("Error writing key: " + termSet, e);
			} 
			
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

