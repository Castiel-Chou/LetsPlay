package qianfengedu.demo05;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: OperatorStateDemo
 * @Description:
 * @Author: Jokey Zhou
 * @Date: 2020/5/19
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class OperatorStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        env.setParallelism(2);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 2000));

        env.setStateBackend(new FsStateBackend(""));

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<Tuple2<String, String>> source = env.addSource(new MyParFileSource(""));

        source.print();

        env.execute("OperatorStateDemo");

    }
}
