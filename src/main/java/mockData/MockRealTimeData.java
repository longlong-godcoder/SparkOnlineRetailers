package mockData;

import conf.ConfigurationManager;
import constant.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class MockRealTimeData implements Runnable{

    private static final Random random = new Random();
    private static final String[] provinces = {"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};
    private static final Map<String, String[]> provinceCityMap = new HashMap<>();
    private KafkaProducer<String, String> producer;

    public MockRealTimeData() {
        provinceCityMap.put("Jiangsu", new String[] {"Nanjing", "Suzhou"});
        provinceCityMap.put("Hubei", new String[] {"Wuhan", "Jingzhou"});
        provinceCityMap.put("Hunan", new String[] {"Changsha", "Xiangtan"});
        provinceCityMap.put("Henan", new String[] {"Zhengzhou", "Luoyang"});
        provinceCityMap.put("Hebei", new String[] {"Shijiazhuang", "Tangshan"});
        //初始化producer
        Properties props = new Properties();
        props.put(Constants.SERIALIZER_CLASS,
                ConfigurationManager.getProperty(Constants.SERIALIZER_CLASS));
        props.put(Constants.METADATA_BROKER_LIST,
                ConfigurationManager.getProperty(Constants.METADATA_BROKER_LIST));
        producer = new KafkaProducer<>(props);
    }
    @Override
    public void run(){
        while (true){
            String province = provinces[random.nextInt(5)];
            String city = provinceCityMap.get(province)[random.nextInt(2)];

            String log = new Date().getTime() + " " + province + " " + city + " "
                    + random.nextInt(1000) + " " + random.nextInt(10);

            producer.send(new ProducerRecord<>("AdRealTimeLog", log));

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Thread thread = new Thread(new MockRealTimeData());
        thread.start();
    }
}
