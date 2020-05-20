package qianfengedu.demo05;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import sun.util.resources.bg.LocaleNames_bg;

import java.io.RandomAccessFile;
import java.util.Collections;

/**
 * @ClassName: MyExactlyOnceParFileSource
 * @Description:  实现自定义的从数据源只读一次的Source
 * @Author: Jokey Zhou
 * @Date: 2020/5/20
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class MyExactlyOnceParFileSource extends RichParallelSourceFunction<Tuple2<String, String>>
        implements CheckpointedFunction {
    private String path;

    private Boolean flag = true;

    private Long offset = 0L;

    private transient ListState<Long> offsetState;

    public MyExactlyOnceParFileSource() {}

    public MyExactlyOnceParFileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

        // 获取offset历史值
        Iterable<Long> iterable = offsetState.get();
        while (iterable.iterator().hasNext()) {
            offset = offsetState.get().iterator().next();
        }

        int subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile randomAccessFile =
                new RandomAccessFile(path + "/" + subTaskIdx + ".txt", "r");

        // 从指定的位置读取数据
        randomAccessFile.seek(offset);

        // 获取一个锁
        final Object lock = ctx.getCheckpointLock();

        while (flag) {
            String line = randomAccessFile.readLine();
            if (line != null) {
                // 进行转码，防止中文乱码
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");

                // 多线程的情况下，为了防止一个线程更新了数据，一个线程又同时在修改数据
                // 因此需要在此处上锁,防止出现线程安全问题
                synchronized (lock) {
                    // 将文件读取到的位置的指针赋给offset
                    offset = randomAccessFile.getFilePointer();
                    // 将数据发送出去
                    ctx.collect(Tuple2.of(subTaskIdx + "", line));
                }
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

    /**
     * 定期将制定的状态数据保存到StateBackend中
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 将历史值清除
        offsetState.clear();
        // 更新最新的状态值
        offsetState.add(offset);

    }



    /**
     * 初始化OperatorState，生命周期方法，构造方法执行后会执行一次
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 定义状态描述器
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>(
                "offset-state",
                Types.LONG);

        // 初始化状态或获取历史状态
        offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
    }
}
