package org.apache.beam.examples.complete.game;

import java.io.IOException;
import java.util.Locale;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutablePeriod;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;
import com.google.common.base.Preconditions;

public class PubsubToGcs {
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
    
    private static final String SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"TrackingAdBQ\",\"namespace\":\"org.apache.beam.examples.complete.game\",\"fields\":[{\"name\":\"uuid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"user_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"client_ad_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ip\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"log_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"tracking_timing\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cue_point_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"cluster_cf_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cluster_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"segment_group_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cue_point_sequence\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"inserted_second\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"inserted_start_datetime\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"inserted_end_datetime\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"slot_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"channel_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"program_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"content_provider_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"placement_method\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"placement_job_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"token_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"account_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"account_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"campaign_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"ad_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"creative_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"creative_asset_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"agency_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"advertiser_brand_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"business_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"appeal_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"goal_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"cm_code\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"promo_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"platform\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"device_model\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"app_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"useragent\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"url\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"portrait\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"position\",\"type\":[\"null\",\"int\"],\"default\":null}]}";
    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
    
    static class AvroDestination extends DynamicAvroDestinations<PubsubMessage,String,TrackingAdBQ>{
    	private static final long serialVersionUID = 1L;
		private final String baseDir;

    	public AvroDestination(String baseDir) {
    		 this.baseDir = baseDir;
    	}

		@Override
		public Schema getSchema(String destination) {
		     return SCHEMA;
		}

		@Override
		public TrackingAdBQ formatRecord(PubsubMessage record) {
			TrackingAdBQ trackingAdbq = null;
			try {
            	PubsubMessage pubsubMessage = record;
                AvroCoder<TrackingAd> coder = AvroCoder.of(TrackingAd.class);
                TrackingAd trackingAd = CoderUtils.decodeFromByteArray(coder, pubsubMessage.getPayload());
                trackingAdbq = TrackingAdMap.TrackingAdToTrackingAdBQ(trackingAd);
            } catch (IOException e) {
                System.out.println("formatRecord:IOException");

                e.printStackTrace();
            }
			return trackingAdbq;
		}

		@Override
		public String getDestination(PubsubMessage record) {
			String ph = record.getAttribute("ph");
			return ph;
		}

		@Override
		public String getDefaultDestination() {
		     return "";
		}

		@Override
		public FilenamePolicy getFilenamePolicy(String destination) {
            String subdir = this.baseDir + destination + "/";
			return new WindowedFilenamePolicy(FileBasedSink.convertToFileResourceIfPossible(subdir));
		}

    }

    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
        ReadDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(ReadDataOptions.class);
        
        Pipeline pipeline = Pipeline.create(options);
		PCollection<PubsubMessage> PubsubReadCollection =
        pipeline
          .apply("Read PubSub Events",
              PubsubIO
              	.readMessagesWithAttributes()
                .withIdAttribute("ph")
                .fromSubscription(options.getTopicSubscript()));
        
        PCollection<PubsubMessage> windowed_items = PubsubReadCollection
          .apply(options.getWindowDuration() + " Watermark Window",
              Window.<PubsubMessage>
                 into(FixedWindows.of(parseDuration(options.getWindowDuration())))
          		);
        
        windowed_items
        .apply("WriteAvros", AvroIO.<PubsubMessage,TrackingAdBQ>writeCustomType().to(new AvroDestination(options.getOutput()))
                .withSchema(SCHEMA)
                .withWindowedWrites()
                .withTempDirectory(StaticValueProvider.of(
                        FileSystems.matchNewResource(options.getOutput(), true)))
        		.withNumShards(options.getNumShards())
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
