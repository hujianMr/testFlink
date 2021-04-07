package com.fadada.log;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonSink extends RichSinkFunction<Tuple4<Long, String, String, String>> {

    static final Logger logger = LoggerFactory.getLogger(NonSink.class);

    @Override
    public void invoke(Tuple4<Long, String, String, String> value, Context context) {
        logger.info("sink value = " + value.toString());
    }
}
