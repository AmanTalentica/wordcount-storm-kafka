package com.example.wordcount;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
public class WordCountBolt extends BaseRichBolt {
    private Map<String, Integer> counts = new HashMap<>();
    private OutputCollector collector;
    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context, OutputCollector collector) { this.collector = collector; }
    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        counts.put(word, counts.getOrDefault(word, 0) + 1);
        collector.emit(new Values(word, counts.get(word)));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new org.apache.storm.tuple.Fields("word","count")); }
}