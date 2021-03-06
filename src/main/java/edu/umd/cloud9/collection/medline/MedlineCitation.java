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

package edu.umd.cloud9.collection.medline;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.uiowa.icts.medline.Citation;
import edu.umd.cloud9.collection.Indexable;

/**
 * Object representing a MEDLINE citation.
 *
 * @author Jimmy Lin
 */
public class MedlineCitation extends Indexable {
	  private static final Logger LOG = Logger.getLogger(MedlineCitation.class);

  /**
   * Start delimiter of the document, which is &lt;<code>MedlineCitation</code> (without closing
   * angle bracket).
   */
  public static final String XML_START_TAG = "<MedlineCitation O";

  /**
   * End delimiter of the document, which is &lt;<code>/MedlineCitation</code>&gt;.
   */
  public static final String XML_END_TAG = "</MedlineCitation>";

  private String pmid;
  private String citation;
  private String title;
  private String abstractText;
  public  Citation getParsedCitation() {
	return parsedCitation;
}

public  void setParsedCitation(Citation parsedCitation) {
	this.parsedCitation = parsedCitation;
}


public void init(String s) {
	InputStream is = null;
	if (s != null) {
		try {
			is = new ByteArrayInputStream(s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			//LOG.error("Error Message", e1);
			//e1.printStackTrace();
		}
	
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
	    try {
			parsedCitation = new Citation((edu.uiowa.medline.xml.MedlineCitation)unmarshaller.unmarshal(is));
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			LOG.error("Error Message", e);
			//LOG.info("XML: " + s);
			//e.printStackTrace();
		}catch (JAXBException e) {
			// TODO Auto-generated catch block
			LOG.error("Error Message", e);
			LOG.info("XML: " + s);
			//e.printStackTrace();
		}
	}
}
private Citation parsedCitation;
  
  
  private static JAXBContext jContext;
  private static Unmarshaller unmarshaller;
  /**
   * Creates an empty {@code MedlineCitation} object.
   */
  public MedlineCitation() {
  }

  /**
   * Deserializes this object.
   */
  public void write(DataOutput out) throws IOException {
    byte[] bytes = citation.getBytes();
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  /**
   * Serializes this object.
   */
  public void readFields(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    MedlineCitation.readCitation(this, new String(bytes));
  }

  /**
   * Returns the docid of this MEDLINE citation, which is its PMID.
   */
  public String getDocid() {
    return getPmid();
  }

  /**
   * Returns the content of this citation, which is the title and abstract.
   */
  public String getContent() {
    return getTitle() + "\n\n" + getAbstract();
  }

  /**
   * Returns the PMID of this MEDLINE citation.
   */
  public String getPmid() {
	  if (parsedCitation != null) {
		  return parsedCitation.getPmid();
	  }
    if (pmid == null) {
      int start = citation.indexOf("<PMID Version=\"1\">");

      if (start == -1) {
        throw new RuntimeException(getRawXML());
      } else {
        int end = citation.indexOf("</PMID>", start);
        pmid = citation.substring(start + 18, end);
      }
    }

    return pmid;
  }

  /**
   * Returns the title of this MEDLINE citation.
   */
  public String getTitle() {
	  if (parsedCitation != null) {
		  return parsedCitation.getArticleTitle();
	  }
    if (title == null) {
      int start = citation.indexOf("<ArticleTitle>");

      if (start == -1) {
        title = "";
      } else {
        int end = citation.indexOf("</ArticleTitle>", start);
        title = citation.substring(start + 14, end);
      }
    }

    return title;
  }

  /**
   * Returns the abstract of this citation.
   */
  public String getAbstract() {
	  if (parsedCitation != null) {
		  return parsedCitation.getAbstractText();
	  }
    if (abstractText == null) {
      int start = citation.indexOf("<AbstractText>");

      if (start == -1) {
        abstractText = "";
      } else {
        int end = citation.indexOf("</AbstractText>", start);
        abstractText = citation.substring(start + 14, end);
      }
    }

    return abstractText;
  }

  /**
   * Returns the raw XML of this citation.
   */
  public String getRawXML() {
    return citation;
  }

  /**
   * Reads a raw XML string into a {@code MedlineCitation} object.
   *
   * @param citation the {@code MedlineCitation} object
   * @param s raw XML string
   */
  public static void readCitation(MedlineCitation citation, String s) {
    Preconditions.checkNotNull(citation);
    Preconditions.checkNotNull(s);
    citation.init(s);

    citation.citation = s;
    citation.pmid = null;
    citation.title = null;
    citation.abstractText = null;
  }
}
