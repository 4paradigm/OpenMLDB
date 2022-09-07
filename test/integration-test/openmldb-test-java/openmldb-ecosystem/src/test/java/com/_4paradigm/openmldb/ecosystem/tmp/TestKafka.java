package com._4paradigm.openmldb.ecosystem.tmp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.util.Collections;
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
//        String message = "{\"data\":[{\"ID\":20,\"UUID\":\"11\",\"PID\":11,\"GID\":11,\"CID\":11}],\"database\":\"d1\",\"table\":\"test_kafka\",\"type\":\"insert\"}";
        String message = "{\"data\":[{\"c1\":\"dd\",\"c2\":1,\"c3\":2,\"c4\":3,\"c5\":1.1,\"c6\":2.2,\"c7\":11,\"c8\":1659512628000,\"c9\":true}],\"type\":\"insert\"}";
//        String message = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":true,\"field\":\"c1\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"c2\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"c3\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"c4\"},{\"type\":\"float\",\"optional\":true,\"field\":\"c5\"},{\"type\":\"double\",\"optional\":true,\"field\":\"c6\"},{\"type\":\"int64\",\"name\":\"org.apache.kafka.connect.data.Date\",\"optional\":true,\"field\":\"c7\"},{\"type\":\"int64\",\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"optional\":true,\"field\":\"c8\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"c9\"}],\"optional\":false,\"name\":\"foobar\"},\"payload\":{\"c1\":\"ee\",\"c2\":1,\"c3\":2,\"c4\":3,\"c5\":1.1,\"c6\":2.2,\"c7\":11,\"c8\":1659512628000,\"c9\":true}}";
        //发送数据
        producer.send(new ProducerRecord<String,String>("m2",message));
//        for (int i=0;i<10;i++){
//            producer.send(new ProducerRecord<String,String>("study","luzelong"+i));
//        }

        //关闭资源
        producer.close();
    }
    @Test
    public void test1() {//自动提交
        //1.创建消费者配置信息
        Properties properties = new Properties();
        //链接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.24.4.55:39092");
        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        //自动提交的延迟
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //key,value的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test-consumer-group1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//重置消费者offset的方法（达到重复消费的目的），设置该属性也只在两种情况下生效：1.上面设置的消费组还未消费(可以更改组名来消费)2.该offset已经过期


        //创建生产者
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("test_kafka")); //Arrays.asList()

        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            //解析并打印consumerRecords
            for (ConsumerRecord consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "----" + consumerRecord.value());
            }
        }

        //consumer无需close()
    }

}
