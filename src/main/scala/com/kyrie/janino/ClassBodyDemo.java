package com.kyrie.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

/**
 * class body
 */
public class ClassBodyDemo {

    public static void main(String[] args) throws CompileException, IllegalAccessException, InstantiationException {
        ClassBodyEvaluator ce = new ClassBodyEvaluator();
        //类体
        String body="public void test(){System.out.println(\"1\");}\n";
        //设置类名
        ce.setClassName("com.kyrie.janino.Test");
        //设置父类
        ce.setExtendedClass(ParentTest.class);
        //编译
        ce.cook("Test.java",body);
        //实例化对象
        ParentTest t = (ParentTest)ce.getClazz().newInstance();
        //调用方法
        t.test();
    }
}


