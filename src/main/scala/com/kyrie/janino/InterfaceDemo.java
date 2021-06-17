package com.kyrie.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.IOException;
import java.io.StringReader;

public class InterfaceDemo {

    public interface Foo{
        int bar(int a, int b);
    }

    public static void main(String[] args) throws IOException, CompileException {
        Foo f = (Foo) ClassBodyEvaluator.createFastClassBodyEvaluator(
                new Scanner(null, new StringReader("public int bar(int a,int b){return a + b;}")),
                Foo.class, // 实现的父类或接口
                (ClassLoader) null // 这里设置为null表示使用当前线程的class loader
        );
        System.out.println("f.bar="+f.bar(1,1));
    }
}
