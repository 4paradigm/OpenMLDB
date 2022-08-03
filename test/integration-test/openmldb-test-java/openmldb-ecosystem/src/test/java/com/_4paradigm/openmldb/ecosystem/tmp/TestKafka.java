package com._4paradigm.openmldb.ecosystem.tmp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.util.Properties;

public class TestKafka {
    @Test
    public void test(){
        //1.创建Kafka生产者的配置信息
        Properties properties = new Properties();
        //指定链接的kafka集群
        properties.put("bootstrap.servers","172.24.4.55:39092");
        //ack应答级别
//        properties.put("acks","all");//all等价于-1   0    1
        //重试次数
        properties.put("retries",1);
        //批次大小
        properties.put("batch.size",16384);//16k
        //等待时间
        properties.put("linger.ms",1);
        //RecordAccumulator缓冲区大小
        properties.put("buffer.memory",33554432);//32m
        //Key,Value的序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//        String message = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int16\",\"optional\":true,\"field\":\"c1_int16\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"c2_int32\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"c3_int64\"},{\"type\":\"float\",\"optional\":true,\"field\":\"c4_float\"},{\"type\":\"double\",\"optional\":true,\"field\":\"c5_double\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"c6_boolean\"},{\"type\":\"string\",\"optional\":true,\"field\":\"c7_string\"},{\"type\":\"int64\",\"name\":\"org.apache.kafka.connect.data.Date\",\"optional\":true,\"field\":\"c8_date\"},{\"type\":\"int64\",\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"optional\":true,\"field\":\"c9_timestamp\"}],\"optional\":false,\"name\":\"foobar\"},\"payload\":{\"c1_int16\":1,\"c2_int32\":2,\"c3_int64\":3,\"c4_float\":4.4,\"c5_double\":5.555,\"c6_boolean\":true,\"c7_string\":\"c77777\",\"c8_date\":19109,\"c9_timestamp\":1651051906000}}";
        String message = "{\"data\":[{\"ID\":16,\"UUID\":\"11\",\"PID\":11,\"GID\":11,\"CID\":11}],\"database\":\"d1\",\"table\":\"test_kafka\",\"type\":\"INSERT\"}";
        //发送数据
        producer.send(new ProducerRecord<String,String>("test_kafka",message));
//        for (int i=0;i<10;i++){
//            producer.send(new ProducerRecord<String,String>("study","luzelong"+i));
//        }

        //关闭资源
        producer.close();
    }
}
