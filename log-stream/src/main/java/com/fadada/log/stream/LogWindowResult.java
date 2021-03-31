package com.fadada.log.stream;

import lombok.Data;

import java.util.List;

@Data
public class LogWindowResult {

    private String appName;

    private List<String> logs;

}
