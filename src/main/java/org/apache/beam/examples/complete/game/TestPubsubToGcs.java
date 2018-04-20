package org.apache.beam.examples.complete.game;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.beam.examples.complete.game.PubsubToGcs.ParseTrackingAdFn;
import org.apache.beam.examples.complete.game.PubsubToGcs.ReadDataOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutablePeriod;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class TestPubsubToGcs {
    public interface ReadDataOptions extends PipelineOptions {

        @Description("Pub/Sub topic subscription to read from")
        @Validation.Required
        String getTopicSubscript();
        void setTopicSubscript(String value);

        @Description("GCS Path of the file to write to")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Description("The window duration in which data will be written. Defaults to 5m. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
        @Default.String("1m")
        String getWindowDuration();
        void setWindowDuration(String value);

        @Description("The maximum number of output shards produced when writing.")
        @Default.Integer(2)
        Integer getNumShards();
        void setNumShards(Integer value);
    }
    
    static class ParseTrackingAdFn extends DoFn<PubsubMessage, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            // Input(json) -> TrackingAd
            try {
            	
            	PubsubMessage pubsubMessage = c.element();
                Map<String,String> map = pubsubMessage.getAttributeMap();
                String attr = pubsubMessage.getAttribute("ph");
                String json = new String(pubsubMessage.getPayload(), "UTF-8");
                System.out.println("attribute:" + attr);

                ObjectMapper mapper = new ObjectMapper();
                TrackingAd trackingAd = mapper.readValue(json, TrackingAd.class);
                c.output(json);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    public static void main(String[] args) throws IOException {
        ReadDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(ReadDataOptions.class);

        Pipeline pipeline = Pipeline.create(options);
		PCollection<PubsubMessage> PubsubReadCollection =
        pipeline
          .apply("Read PubSub Events",
              PubsubIO
              	.readMessagesWithAttributes()
//                .withIdAttribute("ph")
                .withTimestampAttribute("ph")
                .fromSubscription(options.getTopicSubscript()));
        
        PCollection<PubsubMessage> windowed_items = PubsubReadCollection
          .apply(options.getWindowDuration() + " Watermark Window",
              Window.<PubsubMessage>
                 into(FixedWindows.of(parseDuration(options.getWindowDuration())))
                 
                 
       //         .triggering(
       //         		AfterWatermark.pastEndOfWindow()
       //        			.withLateFirings(
       //         					AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardHours(1))
       //         					)
       //        			)
       //         .discardingFiredPanes()
       //   		.withAllowedLateness(Duration.standardHours(1))
          		);
        
        PCollection<String> PubsubtransformData = windowed_items		
                .apply("ParseTrackingAd", ParDo.of(new ParseTrackingAdFn()));

//        PCollection<KV<TrackingAd, String>> windowed_counts = PubsubtransformData.apply(
//        		<String>perElement()
 //       		   Count.<String>perElement());
        		 
        PubsubtransformData.apply("Write File(s)",
              TextIO
                .write()
                .withWindowedWrites()
                .withNumShards(1)
                .to(options.getOutput())
//                .to(new WindowedFilenamePolicy(FileBasedSink.convertToFileResourceIfPossible(options.getOutput())))
 //               .withTempDirectory(null)
        		);
        pipeline.run();
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


}
