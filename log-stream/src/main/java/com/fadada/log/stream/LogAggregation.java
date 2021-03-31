package com.fadada.log.stream;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class LogAggregation {

    private String appName;

    private Long timestamp;

    private LogMetadata log;

    private String logStr;

    private Integer count;
}
