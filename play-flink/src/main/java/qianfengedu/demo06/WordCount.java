package qianfengedu.demo06;

/**
 * @ClassName: WordCount
 * @Description:
 * @Author: Jokey Zhou
 * @Date: 2020/5/23
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class WordCount {

    public String word;

    public Long counts;

    public WordCount() {
    }

    public WordCount(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", counts='" + counts + '\'' +
                '}';
    }
}
