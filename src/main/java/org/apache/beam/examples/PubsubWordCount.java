package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

public class PubsubWordCount {

    // Add commandline options: input (Pub/Sub topic ID) and output (BigQuery table ID).
    public interface MyOptions extends PipelineOptions {
        @Description("PubSub topic to read from, specified as projects/<project_id>/topics/<topic_id>")
        @Validation.Required
        String getInput();
        void setInput(String value);

        @Description("BigQuery table to write to, specified as <project_id>:<dataset_id>.<table_id>")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

    }


    static class MyFormatFunction implements SerializableFunction<String, TableRow> {
        @Override
        public TableRow apply(String input) {
            TableRow tableRow = new TableRow();
            tableRow.set("user_id", input);
            return tableRow;
        }
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        //Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("user_id").setType("STRING"));

        //TableSchema schema = new TableSchema().setFields(fields);

        Pipeline p = Pipeline.create(options);

        PCollection<String> texts = p
                .apply("ReadFromPubSub", PubsubIO.readStrings()
                        .fromTopic(options.getInput()));

        texts.apply("WriteToBigQuery", BigQueryIO
                .<String>write()
                .to(options.getOutput())
                .withFormatFunction(new MyFormatFunction())
                //.withSchema(schema)
                //.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED) // ターゲットテーブルが存在しない場合、テーブル作成
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) // ターゲットテーブルが存在しない場合、書き込み失敗
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)); // 末尾に追加


        p.run();
        // Run the batch pipeline.
        //p.run().waitUntilFinish();    // <- バッチにならなかった

        //System.out.println("Hello World.");

    }

}
