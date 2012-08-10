package edu.uiowa.icts.util;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentMapper extends Mapper<LongWritable, Text, Text, Text>  {
    private static final Logger logger = LoggerFactory.getLogger(IdentMapper.class);

	  private Text raw_xml = new Text();
	  private String pmid = new String();

	  private static enum Count { DOCS };

	  
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
			context.write(new Text(pmid), raw_xml);
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
}

