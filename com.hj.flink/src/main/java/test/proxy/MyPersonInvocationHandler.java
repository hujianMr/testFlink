package test.proxy;

import java.lang.reflect.Method;

public class MyPersonInvocationHandler implements MyInvocationHandler {

    private Object obj;

    public MyPersonInvocationHandler(Object obj) {
        this.obj = obj;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        // TODO Auto-generated method stub
        System.out.println("before time to eat");
        method.invoke(obj, args);
        System.out.println("after time to eat");
        return null;
    }
}
