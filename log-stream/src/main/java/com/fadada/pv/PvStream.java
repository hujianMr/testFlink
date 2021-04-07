package com.fadada.pv;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PvStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("D:\\flink\\testflink\\log-stream\\src\\main\\java\\com\\fadada\\pv\\UserBehavior.csv")
                .map(line -> { // 对数据切割 , 然后封装到 POJO 中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior())) // 过滤出 pv 行为
                .map(behavior -> Tuple2.of("pv", 1L)).returns(Types.TUPLE(Types.STRING,
                Types.LONG)) // 使用 Tuple 类型 , 方便后面求和
                .keyBy(value -> value.f0) // keyBy: 按照 key 分组
                .sum(1) // 求和
                .print();
        env.execute();
    }
}
