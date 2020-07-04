package demo01

/**
 * @ClassName: YieldDemo
 * @Description:
 * @Author: Jokey Zhou
 * @Date: 2020/6/27
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
object YieldDemo {
  def main(args: Array[String]): Unit = {
    val arr = Array(1, 2, 3, 4, 5)

    val r: Array[Int] = for (i <- arr) yield i * 10
    r.foreach(println)
    println("=========================")

    val r1 = for (i <- arr if i % 2 == 0) yield i * 10
    r1.foreach(println)
    println("=========================")
  }
}
