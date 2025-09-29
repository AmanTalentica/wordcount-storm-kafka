# Word Count Topology (Apache Storm)

This project implements a simple word count topology using Apache Storm.

## Structure
- `spout/SentenceSpout.java` – emits random sentences
- `bolt/SplitSentenceBolt.java` – splits sentences into words
- `bolt/WordCountBolt.java` – counts word occurrences
- `topology/WordCountTopology.java` – wires spouts and bolts together

## Build
```bash
mvn clean package
