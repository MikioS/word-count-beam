# word-count-beam


##　dataflow起動 

vaga-load

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.PubsubToGcs \
     -Dexec.args="--runner=DataflowRunner \
                  --project=vega-load \
                  --jobName=vega-pubsubtogcs-20180523 \
                  --labels='{\"env\": \"vega\"}' \
                  --region=asia-east1 \
                  --numWorkers=10 \
                  --windowDuration=5s \
                  --numShards=10 \
                  --workerMachineType=n1-standard-8 \
                  --output=gs://vega-tracking-ad/026/ \
                  --topicSubscript=projects/vega-load/subscriptions/load-to-gcs-tracking-ad" -Pdataflow-runner
```

dev環境

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.PubsubToGcs \
     -Dexec.args="--runner=DataflowRunner \
                  --project=vega-177606 \
                  --jobName=dev-pubsubtogcs-20180523 \
                  --labels='{\"env\": \"dev\"}' \
                  --region=asia-east1 \
                  --numWorkers=1 \
                  --windowDuration=10s \
                  --numShards=2 \
                  --workerMachineType=n1-standard-8 \
                  --output=gs://ito_test_001/dev-to-gcs-tracking-ad/ \
                  --topicSubscript=projects/vega-177606/subscriptions/dev-test-ito-pubsub-to-gcs" -Pdataflow-runner
```


stg環境

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.PubsubToGcs \
     -Dexec.args="--runner=DataflowRunner \
                  --project=vega-177606 \
                  --jobName=stg-pubsubtogcs-20180523 \
                  --labels='{\"env\": \"stg\"}' \
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
                  --jobName=prd-pubsubtogcs-20180523 \
                  --labels='{\"env\": \"prd\"}' \
                  --region=asia-east1 \
                  --numWorkers=10 \
                  --windowDuration=5s \
                  --numShards=10 \
                  --workerMachineType=n1-standard-8 \
                  --output=gs://prd-tracking-ad/ \
                  --topicSubscript=projects/vega-177606/subscriptions/prd-to-gcs-tracking-ad" -Pdataflow-runner
```
