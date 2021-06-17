package com.kyrie.janino;
 
import java.lang.reflect.InvocationTargetException;
 
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;

/**
 * 脚本
 */
public class ScriptDemo {
 
    public static void
    main(String[] args) throws CompileException, NumberFormatException, InvocationTargetException {
 
        ScriptEvaluator se = new ScriptEvaluator();
 
        se.cook(
            ""
            + "static void method1() {\n"
            + "    System.out.println(1);\n"
            + "}\n"
            + "\n"
            + "method1();\n"
            + "method2();\n"
            + "\n"
            + "static void method2() {\n"
            + "    System.out.println(2);\n"
            + "}\n"
        );

        //执行代码块
        se.evaluate(new Object[0]);
    }
}