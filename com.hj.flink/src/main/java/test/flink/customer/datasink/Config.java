package test.flink.customer.datasink;

import lombok.Data;

@Data
public class Config {

    private String label;

    private String remark;

    private String value;

    private String voiceValue;

    protected Config(){}

    public Config(String label, String remark, String value, String voiceValue) {
        this.label = label;
        this.remark = remark;
        this.value = value;
        this.voiceValue = voiceValue;
    }
}
