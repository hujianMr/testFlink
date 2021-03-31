package com.fadada.log.stream;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class LogMetadata {

    private String timestamp;

    private String path;

    private String requestId;

    private String LogLevel;

    private String appName;

    private String hostName;

    private String threadName;

    private String className;

    private String txId;

    private String log;

}
