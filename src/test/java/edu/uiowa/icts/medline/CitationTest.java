/**
 * 
 */
package edu.uiowa.icts.medline;

import static org.junit.Assert.*;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiowa.medline.xml.MedlineCitation;

/**
 * @author schappetj
 *
 */
public class CitationTest extends TestCase {

    private static final Logger logger = LoggerFactory.getLogger(CitationTest.class);
	
	private JAXBContext jContext;

	private Unmarshaller unmarshaller;

	private Citation citation;

	private Citation getCitation() {
		if (citation == null) {
			InputStream is = CitationTest.class.getResourceAsStream("/1000091.xml");
			if (is == null) {
				fail("Did not get Xml File");
			}
			try {
				citation = new Citation((MedlineCitation)unmarshaller.unmarshal(is));
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				fail("JAXB Parse Error");
				logger.error("Parse Error",e);
				//e.printStackTrace();
			}
		}
		return citation;
	}

	private void setCitation(Citation citation) {
		this.citation = citation;
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
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

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		
	}

	/**
	 * Test method for {@link edu.uiowa.icts.medline.Citation#Citation(edu.uiowa.medline.xml.MedlineCitation)}.
	 */
	@Test
	public void testCitation() {
		String title = "Combined modality approach in small cell carcinoma of the bronchus. A pilot study.";
		assertEquals("Did not get title", title,getCitation().getArticleTitle());
	
	}

	/**
	 * Test method for {@link edu.uiowa.icts.medline.Citation#getJournalTitle()}.
	 */
	@Test
	public void testGetJournalTitle() {
		String title = "Bulletin du cancer";
		assertEquals("Did not get Journal title", title,getCitation().getJournalTitle());
	}

	/**
	 * Test method for {@link edu.uiowa.icts.medline.Citation#getArticleTitle()}.
	 */
	@Test
	public void testGetArticleTitle() {
		String title = "Combined modality approach in small cell carcinoma of the bronchus. A pilot study.";
		assertEquals("Did not get title", title,getCitation().getArticleTitle());
	}

	/**
	 * Test method for {@link edu.uiowa.icts.medline.Citation#getPmid()}.
	 */
	@Test
	public void testGetPmid() {
		String pmid = "1000091";
		assertEquals("Did not PMID", pmid,getCitation().getPmid());
	}
	
	@Test
	public void testCountMajorMeshHeadings() {
		assertEquals("Incorrect Number of MeshHeadings", 0, getCitation().getMajorMeshHeading().size());
	}
	@Test
	public void testCountMinorMeshHeadings() {
		assertEquals("Incorrect Number of MeshHeadings", 7, getCitation().getMinorMeshHeading().size());
	}
}
