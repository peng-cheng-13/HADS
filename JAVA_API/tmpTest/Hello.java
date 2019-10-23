public class Hello {

    
    static {
	System.loadLibrary("hello");
    }

    public native void sayHello();
    
    public static void main(String[] args) {
	System.out.println("Hello from java");
	//System.out.println(System.getProperty("java.library.path"));
	//sayHello();
        Hello myclass = new Hello();
        myclass.sayHello();
    }

}
