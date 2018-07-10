import com.lexin.kafka.common.Constants;
import com.lexin.kafka.pool.PoolConfig;
import com.lexin.kafka.pool.kafka.KafkaConnectionPool;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import scala.util.parsing.json.JSONArray;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by lancerlin on 2018/3/15.
 */
public class ProducerTest {
    public static void main(String[] args) {
        addOperateData(null);
    }

    public static void addOperateData(JSONArray recordList){

//        LOG.info("运维数据落地：{}", JSON.toJSONString(recordList));

//        if(recordList == null || recordList.size() == 0){
//            return;
//        }

        PoolConfig config = new PoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);

        /* properties */
        Properties props = new Properties();
        //测试环境ip
        props.setProperty(Constants.METADATA_BROKER_LIST, "10.1.50.122:9092,10.1.50.123:9092,10.1.50.124:9092");
        //生产环境ip
//        props.setProperty(Constants.METADATA_BROKER_LIST, "10.10.16.18:9092,10.10.16.19:9092,10.10.16.20:9092,192.168.16.226:9092,192.168.16.36:9092");
        props.setProperty(Constants.PRODUCER_TYPE, "async");
        props.setProperty(Constants.REQUEST_REQUIRED_ACKS, "1");
        props.setProperty(Constants.COMPRESSION_CODEC, "snappy");
        props.setProperty(Constants.BATCH_NUM_MESSAGES, "200");

        /* connection pool */
        KafkaConnectionPool pool = new KafkaConnectionPool(config, props);

        /* pool getConnection */
        Producer<byte[], byte[]> producer = pool.getConnection();

        /* producer send */
        String topicName = "PT_client_upload_topic";
        String content = "JSONObject.toJSONString(recordList)";
//        String content = JSONObject.toJSONString(recordList);
        String key = UUID.randomUUID().toString();
        System.out.println(producer);
        System.out.println(key + "==" + content);
        producer.send(new KeyedMessage<byte[], byte[]>(topicName,key.getBytes(), content.getBytes()));

        /* pool returnConnection */
        pool.returnConnection(producer);
    }
}
