package com.Demo.kafka;


import org.apache.log4j.Logger;

/**
 * Created by ibf on 08/27.
 */
public class KafkaLog4jTest {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(KafkaLog4jTest.class);
        // TODO:当发送方式为async的时候，有可能出现丢失数据的情况(原因是：程序结束了，但是数据有可能没有发送成功) ==> 如果你的业务要求数据不能丢失的，那么就不要使用log4j的形式发送数据到kafka，采用自定义生产者的方式发送数据到kafka
        for (int i = 0; i < 50; i++) {
            logger.debug("debug-" + i);
            logger.info("info-" + i);
            logger.warn("warn-" + i);
            logger.error("error-" + i);
            logger.fatal("fatal-" + i);
        }

    }
}
