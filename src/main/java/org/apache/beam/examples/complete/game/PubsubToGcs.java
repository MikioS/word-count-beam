package org.apache.beam.examples.complete.game;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.NullValue;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

import com.google.common.base.Preconditions;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.Duration;

import java.io.IOException;


public class PubsubToGcs {
    public interface ReadDataOptions extends PipelineOptions {

        @Description("Pub/Sub topic to read from")
        @Validation.Required
        String getTopic();
        void setTopic(String value);

        @Description("GCS Path of the file to write to")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Description("The window duration in which data will be written. Defaults to 5m. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
        @Default.String("5m")
        String getWindowDuration();
        void setWindowDuration(String value);

        @Description("The maximum number of output shards produced when writing.")
        @Default.Integer(1)
        Integer getNumShards();
        void setNumShards(Integer value);
    }



    public static void main(String[] args) {
        ReadDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(ReadDataOptions.class);

//        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        Pipeline pipeline = Pipeline.create(options);
        pipeline
          .apply("Read PubSub Events",
              PubsubIO
                .readStrings()
                .fromTopic(options.getTopic()))
          .apply(options.getWindowDuration() + " Window", 
              Window
                .into(FixedWindows.of(Duration.standardMinutes(10))))

          .apply("Write File(s)",
              TextIO
                .write()
                .withWindowedWrites()
                .withNumShards(options.getNumShards())
                .to(options.getOutput()));


//                .withFilenamePolicy(
//                    new WindowedFilenamePolicy(
//                        options.getOutputFilenamePrefix(),
//                        options.getShardTemplate(),
//                        options.getOutputFilenameSuffix())
//                    .withSubDirectoryPolicy(options.getSubDirectoryPolicy())));

        // Execute the pipeline and return the result.
        PipelineResult result = pipeline.run();

//        return result;

//        p.apply("Read From PubSub", PubsubIO.readStrings().fromTopic(options.getTopic())
//            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
//            .apply("Write to GCS", TextIO.write().to(options.getOutput())
//                .withWindowedWrites()
//       //         .withFilenamePolicy(new TestPolicy())
//                .withNumShards(10)));

    }
  /**
   * Parses a duration from a period formatted string. Values
   * are accepted in the following formats:
   * <p>
   * Ns - Seconds. Example: 5s<br>
   * Nm - Minutes. Example: 13m<br>
   * Nh - Hours. Example: 2h
   * 
   * <pre>
   * parseDuration(null) = NullPointerException()
   * parseDuration("")   = Duration.standardSeconds(0)
   * parseDuration("2s") = Duration.standardSeconds(2)
   * parseDuration("5m") = Duration.standardMinutes(5)
   * parseDuration("3h") = Duration.standardHours(3)
   * </pre>
   * 
   * @param value The period value to parse.
   * @return  The {@link Duration} parsed from the supplied period string.
   */
/*
  private static Duration parseDuration(String value) {
    Preconditions.checkNotNull(value, "The specified duration must be a non-null value!");

    PeriodParser parser = new PeriodFormatterBuilder()
      .appendSeconds().appendSuffix("s")
      .appendMinutes().appendSuffix("m")
      .appendHours().appendSuffix("h")
      .toParser();

    MutablePeriod period = new MutablePeriod();
    parser.parseInto(period, value, 0, Locale.getDefault());

    Duration duration = period.toDurationFrom(new DateTime(0));
    return duration;
  }
*/
}

