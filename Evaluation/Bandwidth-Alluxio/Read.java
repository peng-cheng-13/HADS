import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import com.google.common.collect.ImmutableMap;
import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.*;
import alluxio.exception.AlluxioException;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.client.file.options.CreateFileOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import alluxio.util.ConfigurationUtils;
import java.util.Properties;

public class Read{

public static void main(String[] arg){
	try{
                String fileID = arg[0];
		String confile = "/BIGDATA/nsccgz_pcheng_1/TDMS-Alluxio/conf/alluxio-site.properties";
	        Properties mypro = ConfigurationUtils.loadPropertiesFromFile(confile);
        	Configuration.merge(mypro);
		FileSystem fs = FileSystem.Factory.get();
                AlluxioURI path = new AlluxioURI("/testfile-java-" + fileID);
	        //AlluxioURI path = new AlluxioURI("/MFS/testfile-java");
		byte b[] =new byte[1048576];
		FileInStream in = fs.openFile(path);
        	System.out.println("FileInStream has created");
       		int read;
        	read = in.read(b);
		System.out.println("Bytearray b[20] = ");
		System.out.print(b[20]);
		System.out.println();
		long startTime = System.currentTimeMillis();
     	   	while(read != -1){
     			read = in.read(b);
		}
        
         	long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        	float bd = 512000/(endTime - startTime);
		System.out.printf("Read Bandwidth = %f MB/s \n",bd);
		
		in.close();    
	}catch(Exception e){
          e.printStackTrace();
        }
}

}
