package manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;

public class HtmlHandler
{
	public static File createHtmlFile(LinkedList<String> oldUrls, LinkedList<String> newUrls, String output) throws IOException
	{
		File outputFile = new File(output);
		BufferedWriter buffWriter = new BufferedWriter(new FileWriter(outputFile));
		buffWriter.write("<html>\n");
		buffWriter.write("<body>\n");

		/* for each message received */
		while(!newUrls.isEmpty())
		{
			String oldUrl = oldUrls.remove();					
			String newUrl = newUrls.remove();
			
			// for each picture: create the new picture with a link to the original picture
			buffWriter.write(" <a href=\""+oldUrl+"\"> <image src=\""+newUrl+"\"> </a> ");
		}

		buffWriter.write("</body>\n");
		buffWriter.write("</html>\n");
		// close
		buffWriter.close();

		return outputFile;
	}

}
