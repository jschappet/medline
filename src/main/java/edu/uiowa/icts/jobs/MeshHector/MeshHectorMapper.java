package edu.uiowa.icts.jobs.MeshHector;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiowa.icts.hector.Client;
import edu.uiowa.icts.medline.Citation;

public class MeshHectorMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, ByteBuffer, List<Mutation>>  {
    private static final Logger LOG = LoggerFactory.getLogger(MeshHectorMapper.class);

      private ByteBuffer sourceColumn;

	private JAXBContext jContext;

	private Unmarshaller unmarshaller;

	  private static enum Count { DOCS, AUTHORS };
	  
	private Client hc;
	private ColumnFamilyTemplate<String, String> meshTemplate;
	    
	  @Override
	  public void setup(Context context) {
		//  outputColumn.set(context.getConfiguration().get("output.column.name"));
		  sourceColumn = ByteBufferUtil.bytes((context.getConfiguration().get("source.column.name")));
		  try {
				jContext=JAXBContext.newInstance("edu.uiowa.medline.xml");
				
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				LOG.error("Error Message", e);
				//e.printStackTrace();
			} 
			
			try {
				unmarshaller = jContext.createUnmarshaller() ;
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				LOG.error("Error Message", e);
				//e.printStackTrace();
			}
			
			hc = new Client("MEDLINE",  "localhost:9160", "ICTS Cluster");
			meshTemplate = hc.getTemplate();
			
	  }
	  
	  
	  @Override
	  public void  cleanup(Context context) {
		  LOG.debug("Docs Processed: " + context.getCounter(Count.DOCS).getValue());
	  }
	  
	  @Override
	  public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) {
		String currentTerm = "";

	  	try {
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
				LOG.error("Error getting value: " , e);
			}
			citation = new Citation(value, unmarshaller);
			List<String> meshList = citation.getMajorMeshHeading();
			for (String mesh : meshList ) {
				currentTerm = mesh;
				//context.getCounter(Count.AUTHORS).increment(1);
				//inserts.add(getMutation(citation.getPmid(), null));
				String uuidTerm = getMeshTermId(mesh, hc.getKeyspace());
				ByteBuffer bytes = ByteBufferUtil.bytes(uuidTerm);
				List<Mutation> meshTerm = new ArrayList<Mutation>();
				
				meshTerm.add(getMutation("term", mesh));
				meshTerm.add(getMutation("exits", 1L));
				try {
					context.write(bytes, meshTerm);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					LOG.error("Error inserting Mesh Term", e1);
				}
				if (bytes != null) {
					try {
						context.write(bytes, Collections.singletonList(getMutation(citation.getPmid(), "1")));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						LOG.error("Error writing Context",e);
					} 
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("error: " + currentTerm, e);
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

	    
	    

	    private static Mutation getMutation(String colName, Long value)
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

	  
		private String getMeshTermId(String term, Keyspace ksp) {
			term = term.replaceAll("'", "''");
			CqlQuery<String,String,String> cqlQuery = new CqlQuery<String,String,String>(ksp, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
			cqlQuery.setQuery("SELECT * FROM Mesh WHERE term = '" + term + "' limit 1");
		
			QueryResult<CqlRows<String,String,String>> result = cqlQuery.execute();
			CqlRows<String, String, String> rowList = result.get();
			if (rowList != null) {
				List<Row<String, String, String>> rowList1 = rowList.getList();
				for (Row<String, String, String> row1 : rowList1) {
					LOG.debug("Found key: " + term + " -- " + row1.getKey());  
						
					return row1.getKey();
				}
			}
			return UUID.randomUUID().toString();
		}
	    
	  
}

