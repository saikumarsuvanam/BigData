
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class Part2 
{
	//main
	public static void main(String[] args) throws IOException
	{
		

		URL url = new URL("https://corpus.byu.edu/wikitext-samples/text.zip");
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		int responseCode = httpConn.getResponseCode();

		if (responseCode == HttpURLConnection.HTTP_OK) {

			
			String source = FilenameUtils.getName(url.getPath());
			String baseSource=FilenameUtils.getBaseName(url.getPath())+".txt";

			InputStream inputStream = httpConn.getInputStream();
			FileOutputStream outputStream = new FileOutputStream(source);

			int bytesRead = -1;
			byte[] buffer = new byte[2048];
			while ((bytesRead = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, bytesRead);
			}

			outputStream.close();
			inputStream.close();

			httpConn.disconnect();
		
		
		
			FileInputStream filesystem = new FileInputStream("text.zip");
			
			ZipInputStream zip = new ZipInputStream(url.openStream());
			
			ZipEntry entry=null;
			
			while ((entry = zip.getNextEntry()) != null)
			{
				byte[] buf2 = new byte[4096];
				int m;
				FileOutputStream fs2 = new FileOutputStream(entry.getName());
			    BufferedOutputStream bs = new BufferedOutputStream(fs2, buf2.length);
			    while ((m = zip.read(buf2, 0, buffer.length)) != -1) 
			    {
			        bs.write(buf2, 0, m);
			    }
			   
			    bs.close();
			}
			
			zip.close();
			filesystem.close();
			FileSystem fileSystem1 = FileSystem.get(new Configuration()); 
			String address = "hdfs://cshadoop1/user/sxs155933/Assignment1/Part2/";
			String destination=address+baseSource;
			Path destinationPath=new Path(destination);
			Path sourcePath=new Path(baseSource);
			fileSystem1.copyFromLocalFile(sourcePath, destinationPath);
		}

	}

}

