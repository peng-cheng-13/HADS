import java.util.Arrays;
//import MemFileSystem;

public class MemFileSystemTest {
    public static void main(String[] args) {
	int ret;
	MemFileSystem myfs = new MemFileSystem();
 	ret = myfs.nrfsConnect();
	if (ret == 0) {
	    System.out.println("File system connected");
	}

	String filepath = "/File1";
	ret = (int) myfs.nrfsOpenFile(filepath.getBytes(), 1);
	if (ret == 0) {
            System.out.println("File created");
        }

	//String writedata = "123456";
        byte[] writedata = {1, 2, 3, 4, 5};
	myfs.nrfsWrite(filepath.getBytes(), writedata, 5, 0);

	String dirpath = "/";
        ret = myfs.nrfsAccess(dirpath.getBytes());
        if (ret == 0) {
	    System.out.printf("Path %s is directory\n", dirpath);
        } else if (ret == 1) {
	    System.out.printf("Not a directory, %s is file\n", dirpath);
	} else {
	    System.out.printf("Path %s not exist\n", dirpath);
	}
	String[] mydata = myfs.nrfsListDirectory(dirpath.getBytes());
        for (int i = 0; i < mydata.length; i++) {
	    System.out.printf("%s\t", mydata[i]);
        }
        System.out.printf("\n");

	int[] fileAttr = new int[2];
        myfs.nrfsGetAttibute(filepath.getBytes(), fileAttr);
	System.out.printf("File %s, size is %d, count of block is %d\n", filepath, fileAttr[0], fileAttr[1]);

	byte[] readData = new byte[10];
        myfs.nrfsRead(filepath.getBytes(), readData, 5, 0);
	System.out.println(Arrays.toString(readData));


	ret = myfs.nrfsDelete(filepath.getBytes());
	if (ret == 0) {
	    System.out.println("File deleted");
	}

	myfs.nrfsDisconnect();
    }
}
