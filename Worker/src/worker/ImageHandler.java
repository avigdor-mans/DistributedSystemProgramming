package worker;

import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import javax.imageio.ImageIO;

public class ImageHandler
{
	public static File createStamp(String url, String fileName) throws IOException
	{
			BufferedImage original = ImageIO.read(new URL(url));

			BufferedImage imgWithText = imgHandle(original);

			File tempFile = new File(fileName);
			ImageIO.write(imgWithText,"png",tempFile);

		return tempFile;
	}


	public static BufferedImage imgHandle(BufferedImage orig) throws IOException
	{
		// declare width and height sizes
		int width = orig.getWidth();
		int height = orig.getHeight();

		// create a new image
		BufferedImage newImg = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

		// declare instance of Graphics2D's class
		Graphics2D g2d = newImg.createGraphics();

		// change the size of origImg
		g2d.drawImage(orig, 0, 0, width, height, null);

		// declare font properties
		g2d.setFont(new Font("Serif", Font.BOLD, 18));
		g2d.setPaint(Color.red);

		// get instance-id 
		String instanceId = getInstanceId();
//		String instanceId = ""; 

		// create date's string
		String date = new java.util.Date().toString();

		// create final image string
		String str = "chenbar, drorven "+ instanceId +" "+ date;
		FontMetrics fm = g2d.getFontMetrics();
		int newWidth = newImg.getWidth() - fm.stringWidth(str) - 5;
		int newHeight = fm.getHeight();

		// Renders (makes) the text applying its attributes
		g2d.drawString(str, newWidth, newHeight);


		// release used memory
		g2d.dispose();

		return newImg;
	}
	
	public static String getInstanceId() throws IOException
	{
		String line;
		URL instanceIdUrl = new URL("http://169.254.169.254/latest/meta-data/instance-id");
		BufferedReader in = new BufferedReader(new InputStreamReader(instanceIdUrl.openStream()));
		line = in.readLine();
		System.out.println(line);
		
        in.close();
        
		return line;
	}

}
