package com.demo.task;

import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.demo.domain.LogEntity;
import com.demo.map.LogMapFunction;

/**
 * 日志 -> Hbase
 *
 * @author XINZE
 */
public class LogTask {

	private final static Logger LOG = LogManager.getLogger(LogTask.class);
	
    public static void main(String[] args) throws Exception {
    	LOG.info("execute: LogTask.main...");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        
//        Properties properties = Property.getKafkaProperties("log");
        Properties properties =new Properties();
        properties.setProperty("bootstrap.servers","kafka:9092");
        properties.setProperty("zookeeper.connect","zk:2181");
        properties.setProperty("group.id","recommendation-test");
//        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset","latest");
        
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
        		"con", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        
        DataStreamSource<String> dataStream = env.addSource(consumer)
        		.setParallelism(1);
        SingleOutputStreamOperator<LogEntity> map = dataStream.map(new LogMapFunction());
        dataStream.print();
        
        LOG.debug("execute: LogTask.main ok.");
        env.execute("LogTask: kafka comsumer");
    }
}
