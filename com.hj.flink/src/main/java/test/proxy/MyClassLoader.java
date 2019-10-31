package test.proxy;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class MyClassLoader extends ClassLoader {

    private File classPathfile;

    public MyClassLoader() {
        String classpth = MyClassLoader.class.getResource("").getPath();
        classPathfile = new File(classpth);
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        String className = MyClassLoader.class.getPackage().getName() + "." +name;
        if (classPathfile != null) {
            File file = new File(classPathfile, name + ".class");
            FileInputStream fileInputStream = null;
            ByteArrayOutputStream outputStream = null;
            try{
                fileInputStream = new FileInputStream(file);
                outputStream = new ByteArrayOutputStream();
                byte[] buff = new byte[1024];
                int len;
                while((len=fileInputStream.read(buff))!=-1){
                    outputStream.write(buff, 0, len);
                }
                return defineClass(className, outputStream.toByteArray(), 0, outputStream.size());
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                if(null!=fileInputStream){
                    try {
                        fileInputStream.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                if(null!=outputStream){
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }
        return null;
    }
}
