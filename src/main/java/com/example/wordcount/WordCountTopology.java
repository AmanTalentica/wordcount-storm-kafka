package com.example.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.Properties;

public class WordCountTopology {
    public static void main(String[] args) {
        try {
            KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder("<BOOTSTRAP_SERVERS>", "sentences")
                    .setProp("group.id", "storm-wordcount")
                    .build();

            KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(spoutConfig);

            Properties boltProps = new Properties();
            boltProps.put("bootstrap.servers", "<BOOTSTRAP_SERVERS>");
            boltProps.put("acks", "1");

            KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
                    .withProducerProperties(boltProps)
                    .withTopicSelector(new DefaultTopicSelector("word-counts"))
                    .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("word", "count"));

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("kafka-spout", kafkaSpout);
            builder.setBolt("split-bolt", new SplitSentenceBolt()).shuffleGrouping("kafka-spout");
            builder.setBolt("count-bolt", new WordCountBolt()).fieldsGrouping("split-bolt", new Fields("word"));
            builder.setBolt("kafka-bolt", kafkaBolt).shuffleGrouping("count-bolt");

            LocalCluster cluster = new LocalCluster();
            Config conf = new Config();
            cluster.submitTopology("wordcount-topology", conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}