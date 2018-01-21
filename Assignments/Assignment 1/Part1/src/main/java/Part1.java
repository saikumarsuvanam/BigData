


import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;
import org.apache.commons.io.FilenameUtils;

public class Part1 {

	// Main
	public static void main(String[] args) throws IOException {

		
		String fileurl = args[0];

		URL url = new URL(fileurl);
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		int responseCode = httpConn.getResponseCode();

		if (responseCode == HttpURLConnection.HTTP_OK) {

			// Step 1
			String source = FilenameUtils.getName(url.getPath());

			InputStream inputStream = httpConn.getInputStream();
			FileOutputStream outputStream = new FileOutputStream(source);

			int bytesRead = -1;
			byte[] buffer = new byte[4096];
			while ((bytesRead = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, bytesRead);
			}

			outputStream.close();
			inputStream.close();

			httpConn.disconnect();
		
		
			// Step 2: File copy from local to hadoop
			String localSrc = source;
			String dst = "hdfs://cshadoop1/user/sxs155933/Assignment1/Part1/" + localSrc;

			InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

			FileSystem fs = FileSystem.get(URI.create(dst), conf);
			OutputStream out = fs.create(new Path(dst), new Progressable() {
				public void progress() {
					System.out.print(".");
				}
			});

			IOUtils.copyBytes(in, out, 4096, true);

			//Step 3:decompress
			String uri1 = dst;
			Configuration conf1 = new Configuration();
			FileSystem fs1 = FileSystem.get(URI.create(uri1), conf);

			Path inputPath = new Path(uri1);
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(inputPath);
			if (codec == null) {
				System.err.println("No codec found for " + uri1);
				System.exit(1);
			}

			String outputUri = CompressionCodecFactory.removeSuffix(uri1, codec.getDefaultExtension());

			InputStream in1 = null;
			OutputStream out1 = null;
			try {
				in1 = codec.createInputStream(fs1.open(inputPath));
				out1 = fs1.create(new Path(outputUri));
				IOUtils.copyBytes(in1, out1, conf);
			} finally {
				IOUtils.closeStream(in1);
				IOUtils.closeStream(out1);
			}
			// step4:delete the Zip file
			FileSystem hdfs = FileSystem.get(conf1);

			hdfs.delete(new Path(dst), false);
		}
	}
}
