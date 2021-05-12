package com.me.gmall.realtime.common;

public class KafkaConfig {
    public static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String ODSTOPIC = "ods_base_log";
    public static final String ODSGROUPID = "base_log_app_group";

    public static final String STARTSINKTOPIC = "dwd_start_log";
    public static final String DISPLAYSINKTOPIC = "dwd_display_log";
    public static final String PAGESINKTOPIC = "dwd_page_log";

}
