public class MemFileSystem {

    
    static {
        System.loadLibrary("jninrfs");
    }

    public native int nrfsConnect();

    public native int nrfsDisconnect();

    public native long nrfsOpenFile(byte[] buf, int flags);

    public native int nrfsCloseFile(byte[] buf);

    public native int nrfsMknod(byte[] buf);

    public native int nrfsAccess(byte[] buf);

    public native int nrfsGetAttibute(byte[] buf, int[] properties);

    public native int nrfsWrite(byte[] path, byte[] buffer, int size, int offset);

    public native int nrfsRead(byte[] path, byte[] buffer, int size, int offset);

    public native int nrfsCreateDirectory(byte[] path);

    public native int nrfsDelete(byte[] path);

    public native int nrfsRename(byte[] oldpath, byte[] newpath);

    public native String[] nrfsListDirectory(byte[] path);

}
