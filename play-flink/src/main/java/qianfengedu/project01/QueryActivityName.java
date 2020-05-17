package qianfengedu.project01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import qianfengedu.flink_utils.UserDefinedFlinkUtils;

/**
 * @ClassName: QueryActivityName
 * @Description: flink关联MySQL进行流式处理
 * @Author: Jokey Zhou
 * @Date: 2020/5/17
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class QueryActivityName {
    public static void main(String[] args) throws Exception {

        DataStreamSource<String> kafkaSource = 
                UserDefinedFlinkUtils.createKafkaSource(args, new SimpleStringSchema());

        SingleOutputStreamOperator<ActivityBean> streamOperator =
                kafkaSource.map(new Data2ActivityBeanFunc());

        streamOperator.print();

        UserDefinedFlinkUtils.getEnv().execute("QueryActivityName");

    }
}
