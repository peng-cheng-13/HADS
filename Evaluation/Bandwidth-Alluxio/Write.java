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
import alluxio.client.file.policy.LocalFirstPolicy;
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

public class Write{

public static void main(String[] arg){
	try{
                String fileID = arg[0];
		String confile = "/BIGDATA/nsccgz_pcheng_1/TDMS-Alluxio/conf/alluxio-site.properties";
        	Properties mypro = ConfigurationUtils.loadPropertiesFromFile(confile);
        	Configuration.merge(mypro);
		CreateFileOptions options = CreateFileOptions.defaults();
		//options = options.setLocationPolicy(new LocalFirstPolicy());
		FileSystem fs = FileSystem.Factory.get();
                AlluxioURI path = new AlluxioURI("/testfile-java-" + fileID);
	        //AlluxioURI path = new AlluxioURI("/MFS/testfile-java");
		byte[] b = new byte [131072]; //best size is 131072
		float bd =0 ;
		int i =0;
	        for(i=0; i<131072;i++){
        	        b[i]=6;
        	}
        	byte c[] = {1,1};
        	
        	FileOutStream out = fs.createFile(path,options);
        	System.out.println("Alluxio has created");
        	long startTime = System.currentTimeMillis();
        	for(i=0; i<4096;i++){ 
                     	out.write(b);
        	}
        	long endTime = System.currentTimeMillis();
		System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
		out.close();
		bd = 512000/(endTime - startTime);
		System.out.printf("Write Bandwidth = %f MB/s \n",bd);
	        System.out.println("Output finished");
	}catch(Exception e){

        }
}

}
