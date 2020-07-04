package demo01

/**
 * @ClassName: ForDemo
 * @Description:
 * @Author: Jokey Zhou
 * @Date: 2020/6/27
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
object ForDemo {
  def main(args: Array[String]): Unit = {

    val str = "abcdef"
    for (i <- str) {
      println(i)
    }

    // 取出字符串中对应角标的字符
    val ch = str(2)
    println(ch)

    /**
     * to是左右闭合区间，until是左闭右开区间
     */
    for (i <- 0 to 5) {
      println(str(i))
    }

    for (i <- 0 until(str.length)) {
      println(str(i))
    }

    println("=========================")

    /**
     * 高级for循环
     * 每个生成器都可以带一个条件，注意：if前面没有分号
     * 下面的写法等于java中的嵌套for循环
     */
    for (i <- 1 to 3; j<- 1 to 3 if 1 != j) {
      print((10 * i + j) + " ")
      println()
    }

  }
}
