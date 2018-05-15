# word-count-beam


##dataflow起動

vaga-load

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.PubsubToGcs \
     -Dexec.args="--runner=DataflowRunner \
                  --project=vega-load \
                  --region=asia-east1 \
                  --numWorkers=10 \
                  --windowDuration=5s \
                  --numShards=10 \
                  --workerMachineType=n1-standard-8 \
                  --output=gs://vega-tracking-ad/026/ \
                  --topicSubscript=projects/vega-load/subscriptions/load-to-gcs-tracking-ad" -Pdataflow-runner
```

stg環境

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.PubsubToGcs \
     -Dexec.args="--runner=DataflowRunner \
                  --project=vega-177606 \
                  --region=asia-east1 \
                  --numWorkers=1 \
                  --windowDuration=10s \
                  --numShards=2 \
                  --workerMachineType=n1-standard-8 \
                  --output=gs://stg-tracking-ad/ \
                  --topicSubscript=projects/vega-177606/subscriptions/stg-to-gcs-tracking-ad" -Pdataflow-runner
```

本番環境

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.PubsubToGcs \
     -Dexec.args="--runner=DataflowRunner \
                  --project=vega-177606 \
                  --region=asia-east1 \
                  --numWorkers=10 \
                  --windowDuration=5s \
                  --numShards=10 \
                  --workerMachineType=n1-standard-8 \
                  --output=gs://prd-tracking-ad/ \
                  --topicSubscript=projects/vega-177606/subscriptions/prd-to-gcs-tracking-ad" -Pdataflow-runner
```
