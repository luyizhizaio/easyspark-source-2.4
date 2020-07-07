package com.kyrie.test

/**
 * Created by tend on 2020/5/25.
 * 测试apply方法
 */
class TestApply {

  def say(): Unit ={
    println("just do it")
  }


}


object TestApply {

  def apply():TestApply={
    println("apply func")
    new TestApply()
  }

  println("i am test apply obj")

}
