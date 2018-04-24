package sam.streams.transfer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RumourProcessor {

    public static void main(String[] args) throws Exception {

        //Properties to hold configuration data about Kafka
        Properties props = new Properties();
        //Name of Application
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        //Kafka server configuration
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //Specifiy the topology
        final StreamsBuilder builder = new StreamsBuilder();
        //Create source stream using topology builder
        KStream<String, String> source = builder.stream("twitter_transfer_stream");
        // Create topology
        final Topology topology = builder.build();

        //Create KafkaStreams using topology and Properties
        final KafkaStreams streams = new KafkaStreams(topology, props);
        // Creates a CountDownLatch
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);


    }
}
