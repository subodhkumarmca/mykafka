package com.saggezza.mykafka;


        import kafka.producer.KeyedMessage;
        import kafka.producer.ProducerConfig;
        import kafka.javaapi.producer.Producer;

        import java.io.*;
        import java.util.Properties;
public class MyKafkaProducer{
    private  static String producerconfigfilepath=null;
    private static String inputfilepath=null;
    private static String topic=null;
    public static void main(String [] args) throws IOException {
        Properties prop = new Properties();
        producerconfigfilepath=args[0];
        inputfilepath=args[1];
        topic=args[2];
        FileInputStream file=new FileInputStream(producerconfigfilepath);
        prop.load(file);
       // prop.put("metadata.broker.list" ,"localhost:9092");
       // prop.put("serializer.class","kafka.serializer.StringEncoder");
        //prop.put("partitioner.class", "example.producer.SimplePartitioner");
        ProducerConfig producerConfig = new ProducerConfig(prop);
        kafka.javaapi.producer.Producer<String,String> producer = new  kafka.javaapi.producer.Producer(producerConfig);
        FileInputStream fileinput=new FileInputStream(inputfilepath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileinput));

        String strLine;
        int c=0;
        while ((strLine = br.readLine()) != null) {
            KeyedMessage<String, String> message = new KeyedMessage(topic,strLine);
            producer.send(message);
            c++;
            System.out.println(message);
        }
        producer.send( new KeyedMessage(topic,"End of File"));
        System.out.print(c);
        producer.close();
        br.close();
    }
}