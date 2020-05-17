package qianfengedu.demo04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StateBackendDemo
 * @Description: flink statebackend demo
 * @Author: Jokey Zhou
 * @Date: 2020/5/17
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class StateBackendDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 只有开启了checkpointing，才会有重启策略
        env.enableCheckpointing(5000);
        // 默认的重启策略是：固定延迟无限重启

        // 此处我们设置重启策略,最多重启几次，每次重启间隔时间
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        // 设置state backend,需要填入hdfs或本地地址
        env.setStateBackend(new FsStateBackend(""));

        // 程序异常退出或人为cancel job，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );


        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map =
                source.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        if (s.startsWith("stop")) {
                            throw new RuntimeException("The Application is Stopped");
                        }
                        return Tuple2.of(s, 1);
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum =
                map.keyBy(0).sum(1);

        sum.print();

        env.execute("StateBackendDemo");
    }
}
