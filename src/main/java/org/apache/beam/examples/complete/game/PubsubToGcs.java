package org.apache.beam.examples.complete.game;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.map.ext.JodaDeserializers.DateTimeDeserializer;
import org.joda.time.format.PeriodParser;
import org.joda.time.MutablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.DateTime;
import java.util.Locale;
import org.apache.avro.Schema;
import com.google.common.base.Preconditions;
import org.joda.time.Duration;

import java.io.File;
import java.io.IOException;

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

/*    
    @DefaultCoder(AvroCoder.class)
    @JsonIgnoreProperties(ignoreUnknown=true)
    static class TrackingAd
    {
    	public String getUuid() {
			return uuid;
		}
		public void setUuid(String uuid) {
			this.uuid = uuid;
		}
		public String getTime() {
			return time;
		}
		public void setTime(String time) {
			this.time = time;
		}
		public String getAction_type() {
			return action_type;
		}
		public void setAction_type(String action_type) {
			this.action_type = action_type;
		}
		public String getPlatform() {
			return platform;
		}
		public void setPlatform(String platform) {
			this.platform = platform;
		}
		@Nullable String uuid;
    	@Nullable String time;		
    	@Nullable String action_type;
    	@Nullable String platform;
    	@Nullable client client;
    	@Nullable page page;
    	@Nullable user user;
    	@Nullable contents contents;

        @JsonIgnoreProperties(ignoreUnknown=true)
    	static class client{
    		@Nullable String device_model;
    		@Nullable String ad_id;
    		@Nullable String os;
    		@Nullable String os_version;
    		@Nullable String ip;
    		@Nullable String useragent;
    		@Nullable String app_version;
   		}

        @JsonIgnoreProperties(ignoreUnknown=true)
    	static class page{
    		public String getUrl() {
				return url;
			}

			public void setUrl(String url) {
				this.url = url;
			}

			@JsonProperty("url")
    		@Nullable String url;
    	}

        @JsonIgnoreProperties(ignoreUnknown=true)
    	static class user{
			@Nullable String service_user_id;
    	}
        @JsonIgnoreProperties(ignoreUnknown=true)
    	static class contents{
    		@Nullable Integer log_type;
    		@Nullable Integer tracking_timing;
    		@Nullable Long cue_point_id;
    		@Nullable Integer cluster_cf_id;
    		@Nullable Integer cluster_id;
    		@Nullable Integer cue_point_sequence;
    		@Nullable Integer inserted_second;
    		@Nullable Long inserted_start_datetime;
    		@Nullable Long inserted_end_datetime;
    		@Nullable String slot_id;
    		@Nullable String channel_id;
    		@Nullable String program_id;
    		@Nullable String content_provider_id;
    		@Nullable Long placement_method;
    		@Nullable String placement_job_id;
    		@Nullable String token_id;
    		@Nullable Long account_id;
    		@Nullable Integer account_type;
    		@Nullable Long campaign_id;
    		@Nullable Long ad_id;
    		@Nullable Long creative_id;
    		@Nullable Long creative_asset_id;
    		@Nullable Long agency_id;
    		@Nullable Long advertiser_brand_id;
    		@Nullable Long business_id;
    		@Nullable Long appeal_id;
    		@Nullable Long goal_id;
    		@Nullable String segment_group_id;
    		@Nullable Boolean portrait;
    		@Nullable Integer background_cluster_cf_id;
    		@Nullable Integer failed_times;
    		@Nullable Integer position;
    		@Nullable String cm_code;
    		@Nullable Integer promo_type;
    	}
    }
*/
    static class ParseTrackingAdFn extends DoFn<String, TrackingAd> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            // Input(json) -> TrackingAd
            try {
                ObjectMapper mapper = new ObjectMapper();
                TrackingAd trackingAd = mapper.readValue(c.element(), TrackingAd.class);
                c.output(trackingAd);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    
 //   private static final String SCHEMA_STRING =
 //   		"{\"type\":\"record\",\"name\":\"TrackingAd\",\"namespace\":\"tv.trk\",\"fields\":[{\"name\":\"uuid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"action_type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"platform\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"client\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Client\",\"namespace\":\"tv.trk\",\"fields\":[{\"name\":\"device_model\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ad_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ip\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"useragent\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"app_version\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"page\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Page\",\"namespace\":\"tv.trk\",\"fields\":[{\"name\":\"url\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"user\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"tv.trk\",\"fields\":[{\"name\":\"service_user_id\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"contents\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Contents\",\"namespace\":\"tv.trk\",\"fields\":[{\"name\":\"log_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"tracking_timing\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cue_point_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"cluster_cf_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cluster_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cue_point_sequence\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"inserted_second\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"inserted_start_datetime\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"inserted_end_datetime\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"slot_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"channel_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"program_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"content_provider_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"placement_method\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"placement_job_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"token_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"account_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"account_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"campaign_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"ad_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"creative_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"creative_asset_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"agency_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"advertiser_brand_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"business_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"appeal_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"goal_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"segment_group_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"portrait\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"background_cluster_cf_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"failed_times\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"position\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cm_code\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"promo_type\",\"type\":[\"null\",\"int\"],\"default\":null}]}],\"default\":null}]}";

//        private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
 
    
    public static void main(String[] args) throws IOException {
        ReadDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(ReadDataOptions.class);

        Schema schema = new Schema.Parser().parse(new File("track.avcs"));
//        Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
        
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> PubsubReadCollection =
        pipeline
          .apply("Read PubSub Events",
              PubsubIO
//              	.readAvros(TrackingAd.class)
              	.readStrings()
                .withTimestampAttribute("ph")
                .fromSubscription(options.getTopicSubscript()));
        
        PCollection<TrackingAd> PubsubtransformData = PubsubReadCollection		
          .apply("ParseTrackingAd", ParDo.of(new ParseTrackingAdFn()));

/*        
        PCollection<String> windowed_json = PubsubReadCollection
                .apply(options.getWindowDuration() + " Watermark Window Json",
                    Window.<String>
                       into(FixedWindows.of(parseDuration(options.getWindowDuration())))
                      .triggering(
                      		AfterWatermark.pastEndOfWindow()
                     			.withLateFirings(
                      					AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardHours(1))
                      					)
                     			)
                      .discardingFiredPanes()
                		.withAllowedLateness(Duration.standardHours(1)));

        PCollection<String> aaa =  windowed_json;
        aaa.apply("Wtite JsonFile(s)",
                		TextIO
                		.write().to(options.getOutput())
                		.withNumShards(options.getNumShards()));
 */      
        PCollection<TrackingAd> windowed_items = PubsubtransformData
          .apply(options.getWindowDuration() + " Watermark Window",
              Window.<TrackingAd>
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
        windowed_items.apply("Write File(s)",
              AvroIO
                .write(TrackingAd.class).to(options.getOutput())
//                .withSchema(schema)
                .withWindowedWrites().to(new WindowedFilenamePolicy(FileBasedSink.convertToFileResourceIfPossible(options.getOutput())))
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


