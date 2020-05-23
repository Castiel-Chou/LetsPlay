package qianfengedu.demo06;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @ClassName: BatchSqlWordCount
 * @Description:  批式flink sql
 * @Author: Jokey Zhou
 * @Date: 2020/5/23
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class BatchSqlWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<WordCount> wordCountDataSource = env.fromElements(
                new WordCount("hadoop", 1L),
                new WordCount("spark", 1L),
                new WordCount("flink", 1L),
                new WordCount("hadoop", 1L),
                new WordCount("flink", 1L)
        );

        // 通过Dataset创建表
        Table table = tEnv.fromDataSet(wordCountDataSource);

        // 调用Table的API进行操作
        Table filtered = table.groupBy("word")
                .select("word, count(1) as counts")
                .filter("counts >= 2")
                .orderBy("counts.desc");

        // 将表转为Dataset
        DataSet<WordCount> dataSet = tEnv.toDataSet(filtered, WordCount.class);

        dataSet.print();
    }
}
