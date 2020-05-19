package qianfengedu.demo05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

/**
 * @ClassName: MyParFileSource
 * @Description:  自定义多并行Source
 * @Author: Jokey Zhou
 * @Date: 2020/5/19
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class MyParFileSource extends RichParallelSourceFunction<Tuple2<String, String>> {

    private String path;

    private Boolean flag = true;

    public MyParFileSource() {}

    public MyParFileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

        int subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile randomAccessFile =
                new RandomAccessFile(path + "/" + subTaskIdx + ".txt", "r");

        while (flag) {
            String line = randomAccessFile.readLine();
            if (line != null) {
                // 进行转码，防止中文乱码
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                // 将数据发送出去
                ctx.collect(Tuple2.of(subTaskIdx + "", line));
            } else {
                // 没有数据的时候 则休眠
                Thread.sleep(1000);
            }
        }

    }

    @Override
    public void cancel() {

        flag = false;

    }
}
