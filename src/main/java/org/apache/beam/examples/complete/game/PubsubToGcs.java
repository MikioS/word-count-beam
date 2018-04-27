package org.apache.beam.examples.complete.game;

import java.io.IOException;
import java.util.Locale;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
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
        @Default.String("1s")
        String getWindowDuration();
        void setWindowDuration(String value);

        @Description("The maximum number of output shards produced when writing.")
        @Default.Integer(2)
        Integer getNumShards();
        void setNumShards(Integer value);
    }
    
    private static final String SCHEMA_STRING = "{\n" + 
    		"  \"namespace\":\"tv.trk\",\n" + 
    		"  \"type\":\"record\",\n" + 
    		"  \"name\":\"tracking_ad\",\n" + 
    		"  \"fields\":[\n" + 
    		"    {\"name\":\"uuid\", \"type\":\"string\"},\n" + 
    		"    {\"name\":\"time\", \"type\":\"string\"},\n" + 
    		"    {\"name\":\"action_type\", \"type\":\"string\"},\n" + 
    		"    {\"name\":\"platform\", \"type\":\"string\"},\n" + 
    		"    {\"name\":\"client\",\n" + 
    		"      \"type\":{\n" + 
    		"        \"type\":\"record\",\n" + 
    		"        \"name\":\"client\",\n" + 
    		"        \"fields\":[\n" + 
    		"          {\"name\":\"device_model\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"ad_id\", \"type\":\"string\", \"default\":\"\"},\n" + 
    		"          {\"name\":\"os\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"os_version\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"ip\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"useragent\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"app_version\", \"type\":\"string\"}\n" + 
    		"        ]\n" + 
    		"      }\n" + 
    		"    },\n" + 
    		"    {\"name\":\"page\",\n" + 
    		"      \"type\":{\n" + 
    		"        \"type\":\"record\",\n" + 
    		"        \"name\":\"page\",\n" + 
    		"        \"fields\":[\n" + 
    		"          {\"name\":\"url\", \"type\":\"string\", \"default\":\"\"}\n" + 
    		"        ]\n" + 
    		"      }\n" + 
    		"    },\n" + 
    		"    {\"name\":\"user\",\n" + 
    		"      \"type\":{\n" + 
    		"        \"type\":\"record\",\n" + 
    		"        \"name\":\"user\",\n" + 
    		"        \"fields\":[\n" + 
    		"          {\"name\":\"service_user_id\", \"type\":\"string\"}\n" + 
    		"        ]\n" + 
    		"      }\n" + 
    		"    },\n" + 
    		"    {\"name\":\"contents\",\n" + 
    		"      \"type\":{\n" + 
    		"        \"type\":\"record\",\n" + 
    		"        \"name\":\"contents\",\n" + 
    		"        \"fields\":[\n" + 
    		"          {\"name\":\"log_type\", \"type\":\"int\"},\n" + 
    		"          {\"name\":\"tracking_timing\", \"type\":\"int\"},\n" + 
    		"          {\"name\":\"cue_point_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"cluster_cf_id\", \"type\":\"int\", \"default\":0},\n" + 
    		"          {\"name\":\"cluster_id\", \"type\":\"int\", \"default\":0},\n" + 
    		"          {\"name\":\"cue_point_sequence\", \"type\":\"int\"},\n" + 
    		"          {\"name\":\"inserted_second\", \"type\":\"int\"},\n" + 
    		"          {\"name\":\"inserted_start_datetime\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"inserted_end_datetime\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"slot_id\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"channel_id\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"program_id\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"content_provider_id\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"placement_method\", \"type\":\"long\", \"default\":0},\n" + 
    		"          {\"name\":\"placement_job_id\", \"type\":\"string\", \"default\":\"\"},\n" + 
    		"          {\"name\":\"token_id\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"account_id\", \"type\": \"long\"},\n" + 
    		"          {\"name\":\"account_type\", \"type\":\"int\"},\n" + 
    		"          {\"name\":\"campaign_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"ad_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"creative_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"creative_asset_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"agency_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"advertiser_brand_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"business_id\", \"type\":\"long\"},\n" + 
    		"          {\"name\":\"appeal_id\", \"type\":\"long\", \"default\":0},\n" + 
    		"          {\"name\":\"goal_id\", \"type\":\"long\", \"default\":0},\n" + 
    		"          {\"name\":\"segment_group_id\", \"type\":\"string\", \"default\":\"\"},\n" + 
    		"          {\"name\":\"portrait\", \"type\":\"boolean\"},\n" + 
    		"          {\"name\":\"background_cluster_cf_id\", \"type\":\"int\", \"default\":0},\n" + 
    		"          {\"name\":\"failed_times\", \"type\":\"int\", \"default\":0},\n" + 
    		"          {\"name\":\"position\", \"type\":\"int\", \"default\":-1},\n" + 
    		"          {\"name\":\"cm_code\", \"type\":\"string\"},\n" + 
    		"          {\"name\":\"promo_type\", \"type\":\"int\", \"default\":-1}\n" + 
    		"        ]\n" + 
    		"      }\n" + 
    		"    }\n" + 
    		"  ]\n" + 
    		"}";
    		private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
    
    static class AvroDestination extends DynamicAvroDestinations<PubsubMessage,String,GenericRecord>{
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
		public GenericRecord formatRecord(PubsubMessage record) {
			GenericRecord trackingAd = null;
        	PubsubMessage pubsubMessage = record;
            AvroCoder<GenericRecord> coder = AvroCoder.of(GenericRecord.class,SCHEMA);
            try {
				trackingAd = CoderUtils.decodeFromByteArray(coder, pubsubMessage.getPayload());
			} catch (CoderException e) {
				e.printStackTrace();
			}
			return trackingAd;
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
        pipeline
          .apply("Read PubSub Events",
              PubsubIO
              	.readMessagesWithAttributes()
                .fromSubscription(options.getTopicSubscript()))
        
          .apply(options.getWindowDuration() + " Watermark Window",
              Window.<PubsubMessage>
                 into(FixedWindows.of(parseDuration(options.getWindowDuration()))))

        .apply("WriteAvros", AvroIO.<PubsubMessage>writeCustomTypeToGenericRecords().to(new AvroDestination(options.getOutput()))
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
