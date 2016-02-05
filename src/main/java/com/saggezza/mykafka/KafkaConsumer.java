package com.saggezza.mykafka;


        import java.io.UnsupportedEncodingException;
        import java.nio.ByteBuffer;
        import java.util.HashMap;
        import java.util.List;
        import java.util.Map;
        import java.util.Properties;
        import kafka.consumer.Consumer;
        import kafka.consumer.ConsumerConfig;
        import kafka.consumer.ConsumerIterator;
        import kafka.consumer.KafkaStream;
        import kafka.javaapi.consumer.ConsumerConnector;
        import kafka.javaapi.message.ByteBufferMessageSet;
        import kafka.message.MessageAndOffset;


/**
 * Created by user on 8/4/14.
 */
public class KafkaConsumer  {
    //final static String clientId = "SimpleConsumerDemoClient";
    //final static String TOPIC = "pythontest";
    ConsumerConnector consumerConnector;
    public static void main(String[] argv) throws UnsupportedEncodingException {
        KafkaConsumer helloKafkaConsumer = new KafkaConsumer();
        helloKafkaConsumer.start1();
    }
    public KafkaConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","test-group");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void start1() throws UnsupportedEncodingException {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("Employe",new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("Employe").get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while(it.hasNext()) {
            System.out.println(it.next().message());
            //System.out.println(new String(it.next().message(), "UTF-8"));
        }

       /* SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", 2181, 600000, 10000, "group");
        System.out.println("Testing single fetch");
        FetchRequest req = new FetchRequestBuilder()
                .clientId("group")
                .addFetch("text", 0, 0L,50)
                .build();
        FetchResponse fetchResponse = simpleConsumer.fetch(req);
        printMessages((ByteBufferMessageSet) fetchResponse.messageSet("text", 0));*/


    }
    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }
}