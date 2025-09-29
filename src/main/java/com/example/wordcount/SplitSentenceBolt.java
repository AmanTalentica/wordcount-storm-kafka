package com.example.wordcount;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
public class SplitSentenceBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context, OutputCollector collector) { this.collector = collector; }
    @Override
    public void execute(Tuple input) {
        String sentence = input.getStringByField("value");
        for(String word : sentence.split("\\s+")) { collector.emit(new Values(word.toLowerCase())); }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new org.apache.storm.tuple.Fields("word")); }
}