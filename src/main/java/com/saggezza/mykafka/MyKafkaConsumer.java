package com.saggezza.mykafka;
import java.io.*;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Created by user on 8/4/14.
 */
public class MyKafkaConsumer extends Thread {
    private ConsumerConnector consumerConnector;
    private static String topic=null;
    private static String consumerconfigfilepath=null;
    MyHDFSFileCreation hdfsFileCreation;
    public static void main(String[] argv) throws IOException, URISyntaxException {
        consumerconfigfilepath=argv[0];
       topic=argv[1];
        MyKafkaConsumer helloKafkaConsumer = new MyKafkaConsumer();
        helloKafkaConsumer.start();
    }

    public MyKafkaConsumer() throws IOException, URISyntaxException {

        Properties properties = new Properties();
      //  properties.put("zookeeper.connect", "localhost:2181");
       // properties.put("group.id", "test-consumer-group");
        FileInputStream file=new FileInputStream(consumerconfigfilepath);
        properties.load(file);
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        hdfsFileCreation=MyHDFSFileCreation.creat();
    }

    public void run() {

        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        // Define single thread for topic
        topicMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap =
                consumerConnector.createMessageStreams(topicMap);

        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap
                .get(topic);

        for (final KafkaStream<byte[], byte[]> stream : streamList) {
            ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
            while (consumerIte.hasNext()) {

                // System.out.println("Message from Single Topic :: "+ new String(consumerIte.next().message()));
                String line=new String(consumerIte.next().message());
                if(line.equals("End of File"))
                {
                    try {
                        hdfsFileCreation.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                }
                try
                {
                    hdfsFileCreation.append(line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
