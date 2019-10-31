package test.proxy;

public class TestProxy {
    public static void main(String[] args) {
        MyPersonInvocationHandler personInvocationHandler = new MyPersonInvocationHandler(
                new PersonImpl());
        Person personProxy = (Person) MyProxy.newProxyInstance(
                new MyClassLoader(), PersonImpl.class.getInterfaces(),
                personInvocationHandler);
        personProxy.eat();
    }
}
