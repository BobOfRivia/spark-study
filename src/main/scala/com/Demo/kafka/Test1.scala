package com.Demo.kafka

class Person(
              name: String,
              age: Int) {
  def this(name: String) {
    this(name, 12)
  }

  def f(): Unit = {
    //    Person.a
    println("name:" + name)
  }
}

object Person {
  //  private val a = "123"
  def apply(name: String, age: Int): Person = new Person(name, age)
}

object Student {
  def apply(name: String, age: Int): Person = new Person(name, age)
}

class A

// 其实case class其实就是在构建class的同时构建一个伴生对象，并同时产生一个主构造函数同输入参数的apply方法(object中) ===> 为了方便对象的构建
case class Teacher(name: String, age: Int) {
  def f(): Unit = {
    println("name=" + name)
  }
}

object Test1 {
  def matchTeach(t: Teacher): Unit = {
    t match {
      case Teacher("xiaoming", _) => {
        println("xiaoming....")
      }
      case Teacher(_, age) if age % 2 == 0 => {
        println("age is=" + age)
      }
      case Teacher(name, age) => {
        println("name is " + name + ", age is " + age)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val t = Teacher("xiaoliu", 18)


  }
}