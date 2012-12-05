package storm.dsp.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import org.drools.decisiontable.ExternalSpreadsheetCompiler;
import org.drools.io.ResourceFactory;

/**
 * This class is used to compile "XLS+DRT" Rules Base into a single "DRL" Rules Base
 * Because DRL rule base could be imported into the Guvnor GUI
 * @author Tony WANG
 *
 */
public class FromTemplateToDRL {

	public static void main(String[] args) {
		String drl = compile("dsp-DL-evname.xls", "dsp-DI.drt");
		try {
			// File file = new File("C:\\dsp.drl");
			// BufferedWriter out = new BufferedWriter(new FileWriter(file));
			BufferedWriter out = new BufferedWriter(new FileWriter(
					"src\\main\\resources\\dsp-DL-evname.drl"));
			out.write(drl);
			out.close();
		} catch (IOException e) {
			System.out.println("Exception ");
		}
		System.out.println(drl);
	}

	public static String compile(String xls, String drt) {
		// TODO Auto-generated method stub
		final ExternalSpreadsheetCompiler converter = new ExternalSpreadsheetCompiler();

		// the data we are interested in starts at row 2, column 2 (e.g. B2)
		String drl = null;
		try {
			drl = converter.compile(getSpreadsheetStream(xls),
					getRulesStream(drt), 2, 1);
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"Could not read spreadsheet or rules stream.", e);
		}
		return drl;

	}

	private static InputStream getSpreadsheetStream(String xls)
			throws IOException {
		return ResourceFactory.newClassPathResource(xls).getInputStream();
	}

	private static InputStream getRulesStream(String drt) throws IOException {
		return ResourceFactory.newClassPathResource(drt).getInputStream();
	}

}
