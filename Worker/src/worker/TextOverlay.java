package worker;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;

import javax.imageio.ImageIO;
import javax.swing.JPanel;

public class TextOverlay extends JPanel
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7179782459852897002L;
	private BufferedImage image;
	
	public TextOverlay()
	{
        try
        {
            image = ImageIO.read(new URL(
                "http://sstatic.net/so/img/logo.png"));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        this.setPreferredSize(new Dimension(
            image.getWidth(), image.getHeight()));
        image = process(image);
    }
	
	private BufferedImage process(BufferedImage old)
	{
        int w = old.getWidth();
        int h = old.getHeight();
        BufferedImage img = new BufferedImage(
            w, h, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = img.createGraphics();
        g2d.drawImage(old, 0, 0, null);
        g2d.setPaint(Color.red);
        g2d.setFont(new Font("Serif", Font.BOLD, 20));
        String s = "Hello, world!";
        FontMetrics fm = g2d.getFontMetrics();
        int x = img.getWidth() - fm.stringWidth(s) - 5;
        int y = fm.getHeight();
        g2d.drawString(s, x, y);
        g2d.dispose();
        return img;
    }
	
	
}
