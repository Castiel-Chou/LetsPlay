package demo01

/**
 * @ClassName: IfDemo
 * @Description: if demo
 * @Author: Jokey Zhou
 * @Date: 2020/6/27
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
object IfDemo {
  def main(args: Array[String]): Unit = {

    var m = 10
    if (m > 0) {
      println("bigger")
      println("big")
    } else {
      println("smaller")
    }

    if (m < -1) {
      println("A")
    } else if (m > 5) {
      println("B")
    } else {
      println("C")
    }

    // 此处需要注意的是：if...else两者的返回值类型可以不同
    val i: Any = if (m > 0) 100 else "abc"
    println(i)

  }
}
