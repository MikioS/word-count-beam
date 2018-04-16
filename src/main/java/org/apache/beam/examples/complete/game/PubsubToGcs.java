package org.apache.beam.examples.complete.game;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.joda.time.format.PeriodParser;
import org.joda.time.MutablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.DateTime;
import java.util.Locale;
import org.apache.avro.Schema;
import com.google.common.base.Preconditions;
import org.joda.time.Duration;
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
        @Default.Integer(1)
        Integer getNumShards();
        void setNumShards(Integer value);
    }


    @DefaultCoder(AvroCoder.class)
    static class TrackingAd {
    	
    	@Nullable String uuid;
    	@Nullable Integer time;
    	@Nullable String user_id;
    	@Nullable String client_ad_id;
    	@Nullable String os;
    	@Nullable String ip;
    	@Nullable Integer log_type;
    	@Nullable Integer tracking_timing;
    	@Nullable String cue_point_id;
    	@Nullable Integer cluster_cf_id;
    	@Nullable Integer cluster_id;
    	@Nullable String segment_group_ids;
    	@Nullable Integer cue_point_sequence;
    	@Nullable Integer inserted_second;
    	@Nullable Integer inserted_start_datetime;
    	@Nullable Integer inserted_end_datetime;
    	@Nullable String slot_id;
    	@Nullable String channel_id;
    	@Nullable String program_id;
    	@Nullable String content_provider_id;
    	@Nullable Integer placement_method;
    	@Nullable String placement_job_id;
    	@Nullable String token_id;
    	@Nullable Integer account_id;
    	@Nullable Integer account_type;
    	@Nullable Integer campaign_id;
    	@Nullable Integer ad_id;
    	@Nullable Integer creative_id;
    	@Nullable Integer creative_asset_id;
    	@Nullable Integer agency_id;
    	@Nullable Integer advertiser_brand_id;
    	@Nullable Integer business_id;
    	@Nullable Integer appeal_id;
    	@Nullable Integer goal_id;
    	@Nullable String cm_code;
    	@Nullable Integer promo_type;
    	@Nullable String platform;
    	@Nullable String device_model;
    	@Nullable String os_version;
    	@Nullable String app_version;
    	@Nullable String useragent;
    	@Nullable String url;
    	@Nullable Boolean portrait;
    	@Nullable Integer position;
    	public Integer getTime() {
			return time;
		}

		public void setTime(Integer time) {
			this.time = time;
		}

		public String getUser_id() {
			return user_id;
		}

		public void setUser_id(String user_id) {
			this.user_id = user_id;
		}

		public String getClient_ad_id() {
			return client_ad_id;
		}

		public void setClient_ad_id(String client_ad_id) {
			this.client_ad_id = client_ad_id;
		}

		public String getOs() {
			return os;
		}

		public void setOs(String os) {
			this.os = os;
		}

		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

		public Number getLog_type() {
			return log_type;
		}

		public void setLog_type(Integer log_type) {
			this.log_type = log_type;
		}

		public Number getTracking_timing() {
			return tracking_timing;
		}

		public void setTracking_timing(Integer tracking_timing) {
			this.tracking_timing = tracking_timing;
		}

		public String getCue_point_id() {
			return cue_point_id;
		}

		public void setCue_point_id(String cue_point_id) {
			this.cue_point_id = cue_point_id;
		}

		public Number getCluster_cf_id() {
			return cluster_cf_id;
		}

		public void setCluster_cf_id(Integer cluster_cf_id) {
			this.cluster_cf_id = cluster_cf_id;
		}

		public Number getCluster_id() {
			return cluster_id;
		}

		public void setCluster_id(Integer cluster_id) {
			this.cluster_id = cluster_id;
		}

		public String getSegment_group_ids() {
			return segment_group_ids;
		}

		public void setSegment_group_ids(String segment_group_ids) {
			this.segment_group_ids = segment_group_ids;
		}

		public Number getCue_point_sequence() {
			return cue_point_sequence;
		}

		public void setCue_point_sequence(Integer cue_point_sequence) {
			this.cue_point_sequence = cue_point_sequence;
		}

		public Number getInserted_second() {
			return inserted_second;
		}

		public void setInserted_second(Integer inserted_second) {
			this.inserted_second = inserted_second;
		}

		public Number getInserted_start_datetime() {
			return inserted_start_datetime;
		}

		public void setInserted_start_datetime(Integer inserted_start_datetime) {
			this.inserted_start_datetime = inserted_start_datetime;
		}

		public Number getInserted_end_datetime() {
			return inserted_end_datetime;
		}

		public void setInserted_end_datetime(Integer inserted_end_datetime) {
			this.inserted_end_datetime = inserted_end_datetime;
		}

		public String getSlot_id() {
			return slot_id;
		}

		public void setSlot_id(String slot_id) {
			this.slot_id = slot_id;
		}

		public String getChannel_id() {
			return channel_id;
		}

		public void setChannel_id(String channel_id) {
			this.channel_id = channel_id;
		}

		public String getProgram_id() {
			return program_id;
		}

		public void setProgram_id(String program_id) {
			this.program_id = program_id;
		}

		public String getContent_provider_id() {
			return content_provider_id;
		}

		public void setContent_provider_id(String content_provider_id) {
			this.content_provider_id = content_provider_id;
		}

		public Number getPlacement_method() {
			return placement_method;
		}

		public void setPlacement_method(Integer placement_method) {
			this.placement_method = placement_method;
		}

		public String getPlacement_job_id() {
			return placement_job_id;
		}

		public void setPlacement_job_id(String placement_job_id) {
			this.placement_job_id = placement_job_id;
		}

		public String getToken_id() {
			return token_id;
		}

		public void setToken_id(String token_id) {
			this.token_id = token_id;
		}

		public Number getAccount_id() {
			return account_id;
		}

		public void setAccount_id(Integer account_id) {
			this.account_id = account_id;
		}

		public Number getAccount_type() {
			return account_type;
		}

		public void setAccount_type(Integer account_type) {
			this.account_type = account_type;
		}

		public Number getCampaign_id() {
			return campaign_id;
		}

		public void setCampaign_id(Integer campaign_id) {
			this.campaign_id = campaign_id;
		}

		public Number getAd_id() {
			return ad_id;
		}

		public void setAd_id(Integer ad_id) {
			this.ad_id = ad_id;
		}

		public Number getCreative_id() {
			return creative_id;
		}

		public void setCreative_id(Integer creative_id) {
			this.creative_id = creative_id;
		}

		public Integer getCreative_asset_id() {
			return creative_asset_id;
		}

		public void setCreative_asset_id(Integer creative_asset_id) {
			this.creative_asset_id = creative_asset_id;
		}

		public Number getAgency_id() {
			return agency_id;
		}

		public void setAgency_id(Integer agency_id) {
			this.agency_id = agency_id;
		}

		public Integer getAdvertiser_brand_id() {
			return advertiser_brand_id;
		}

		public void setAdvertiser_brand_id(Integer advertiser_brand_id) {
			this.advertiser_brand_id = advertiser_brand_id;
		}

		public Integer getBusiness_id() {
			return business_id;
		}

		public void setBusiness_id(Integer business_id) {
			this.business_id = business_id;
		}

		public Number getAppeal_id() {
			return appeal_id;
		}

		public void setAppeal_id(Integer appeal_id) {
			this.appeal_id = appeal_id;
		}

		public Integer getGoal_id() {
			return goal_id;
		}

		public void setGoal_id(Integer goal_id) {
			this.goal_id = goal_id;
		}

		public String getCm_code() {
			return cm_code;
		}

		public void setCm_code(String cm_code) {
			this.cm_code = cm_code;
		}

		public Number getPromo_type() {
			return promo_type;
		}

		public void setPromo_type(Integer promo_type) {
			this.promo_type = promo_type;
		}

		public String getPlatform() {
			return platform;
		}

		public void setPlatform(String platform) {
			this.platform = platform;
		}

		public String getDevice_model() {
			return device_model;
		}

		public void setDevice_model(String device_model) {
			this.device_model = device_model;
		}

		public String getOs_version() {
			return os_version;
		}

		public void setOs_version(String os_version) {
			this.os_version = os_version;
		}

		public String getApp_version() {
			return app_version;
		}

		public void setApp_version(String app_version) {
			this.app_version = app_version;
		}

		public String getUseragent() {
			return useragent;
		}

		public void setUseragent(String useragent) {
			this.useragent = useragent;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public Boolean getPortrait() {
			return portrait;
		}

		public void setPortrait(Boolean portrait) {
			this.portrait = portrait;
		}

		public Integer getPosition() {
			return position;
		}

		public void setPosition(Integer position) {
			this.position = position;
		}


        public TrackingAd() {}

		public String getUuid() {
			return uuid;
		}

		public void setUuid(String uuid) {
			this.uuid = uuid;
		}
    }

    static class ParseTrackingAdFn extends DoFn<String, TrackingAd> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            // Input(json) -> TrackingAd
            try {
                ObjectMapper mapper = new ObjectMapper();
                //System.out.println("Input Json: "+c.element());

                JsonNode node = mapper.readTree(c.element());
                TrackingAd trackingAd = new TrackingAd();
                trackingAd.setUuid(node.get("uuid").asText());
                String timetext = node.get("time").asText();
                int time_unittime = (int)((DateTime.parse(timetext, DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))).getMillis() / 1000);
                trackingAd.setTime(time_unittime);
                trackingAd.setUser_id(node.get("user").get("service_user_id").asText());
                trackingAd.setClient_ad_id(node.get("client").get("ad_id").asText());
                trackingAd.setOs(node.get("client").get("os").asText());
                trackingAd.setIp(node.get("client").get("ip").asText());
                trackingAd.setLog_type(node.get("contents").get("log_type").asInt());
                trackingAd.setTracking_timing(node.get("contents").get("tracking_timing").asInt());
                trackingAd.setCue_point_id(node.get("contents").get("cue_point_id").asText());
                trackingAd.setCluster_cf_id(node.get("contents").get("cluster_cf_id").asInt());
                trackingAd.setCluster_id(node.get("contents").get("cluster_id").asInt());
                trackingAd.setSegment_group_ids(node.get("contents").get("segment_group_id").asText());
                trackingAd.setCue_point_sequence(node.get("contents").get("cue_point_sequence").asInt());
                trackingAd.setInserted_second(node.get("contents").get("inserted_second").asInt());
                trackingAd.setInserted_start_datetime(node.get("contents").get("inserted_start_datetime").asInt());
                trackingAd.setInserted_end_datetime(node.get("contents").get("inserted_end_datetime").asInt());
                trackingAd.setSlot_id(node.get("contents").get("slot_id").asText());
                trackingAd.setChannel_id(node.get("contents").get("channel_id").asText());
                trackingAd.setProgram_id(node.get("contents").get("program_id").asText());
                trackingAd.setContent_provider_id(node.get("contents").get("content_provider_id").asText());
                trackingAd.setPlacement_method(node.get("contents").get("placement_method").asInt());
                trackingAd.setPlacement_job_id(node.get("contents").get("placement_job_id").asText());
                trackingAd.setToken_id(node.get("contents").get("token_id").asText());
                trackingAd.setAccount_id(node.get("contents").get("account_id").asInt());
                trackingAd.setAccount_type(node.get("contents").get("account_type").asInt());                
                trackingAd.setCampaign_id(node.get("contents").get("campaign_id").asInt());
                trackingAd.setAd_id(node.get("contents").get("ad_id").asInt());
                trackingAd.setCreative_id(node.get("contents").get("creative_id").asInt());
                trackingAd.setCreative_asset_id(node.get("contents").get("creative_asset_id").asInt());
                trackingAd.setAgency_id(node.get("contents").get("agency_id").asInt());
                trackingAd.setAdvertiser_brand_id(node.get("contents").get("advertiser_brand_id").asInt());
                trackingAd.setBusiness_id(node.get("contents").get("business_id").asInt());
                trackingAd.setAppeal_id(node.get("contents").get("appeal_id").asInt());
                trackingAd.setGoal_id(node.get("contents").get("goal_id").asInt());
                trackingAd.setCm_code(node.get("contents").get("cm_code").asText());
                trackingAd.setPromo_type(node.get("contents").get("promo_type").asInt());
                trackingAd.setPlatform(node.get("platform").asText());
                trackingAd.setDevice_model(node.get("client").get("device_model").asText());
                trackingAd.setOs_version(node.get("client").get("os_version").asText());
                trackingAd.setApp_version(node.get("client").get("app_version").asText());
                trackingAd.setUseragent(node.get("client").get("useragent").asText());
                trackingAd.setUrl(node.get("page").get("url").asText());
                trackingAd.setPortrait(node.get("contents").get("portrait").asBoolean());
                trackingAd.setPosition(node.get("contents").get("position").asInt());
                              
//                TrackingAd trackingAd = mapper.readValue(c.element(), TrackingAd.class);
                c.output(trackingAd);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static final String SCHEMA_STRING =
        "{\"namespace\": \"trackingad.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"TrackingAd\",\n"
            + " \"fields\": [\n"
            + "       {\"name\":\"uuid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"user_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"client_ad_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ip\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"log_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"tracking_timing\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cue_point_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cluster_cf_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cluster_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"segment_group_ids\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cue_point_sequence\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"inserted_second\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"inserted_start_datetime\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"inserted_end_datetime\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"slot_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"channel_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"program_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"content_provider_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"placement_method\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"placement_job_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"token_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"account_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"account_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"campaign_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"ad_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"creative_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"creative_asset_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"agency_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"advertiser_brand_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"business_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"appeal_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"goal_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"cm_code\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"promo_type\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"platform\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"device_model\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"app_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"useragent\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"url\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"portrait\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"position\",\"type\":[\"null\",\"int\"],\"default\":null}"
            + " ]\n"
            + "}";

//    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);


    public static void main(String[] args) {
        ReadDataOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(ReadDataOptions.class);

        Schema schema = new Schema.Parser().parse(SCHEMA_STRING);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> PubsubReadCollection =
        pipeline
          .apply("Read PubSub Events",
              PubsubIO
                .readStrings()
                .withTimestampAttribute("timestamp")
                .fromSubscription(options.getTopicSubscript()));
        
        PCollection<TrackingAd> PubsubtransformData = PubsubReadCollection		
          .apply("ParseTrackingAd", ParDo.of(new ParseTrackingAdFn()));

        PCollection<TrackingAd> windowed_items = PubsubtransformData
          .apply(options.getWindowDuration() + " Watermark Window",
              Window.<TrackingAd>
                 into(FixedWindows.of(parseDuration(options.getWindowDuration())))
                .triggering(
                		AfterWatermark.pastEndOfWindow()
               			.withLateFirings(
                					AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardHours(1))
                					)
               			)
                .discardingFiredPanes()
          		.withAllowedLateness(Duration.standardHours(1))
          		);
/*
        PCollection<TrackingAd> window_pc = PubsubtransformData;	
        window_pc.apply(options.getWindowDuration() + " Window",
                Window.into(FixedWindows.of(parseDuration(options.getWindowDuration()))));
*/
        PCollection<TrackingAd> avro_pc = windowed_items;
        
        
        windowed_items.apply("Write File(s)",
              AvroIO
                .write(TrackingAd.class).to(options.getOutput())               
                .withSchema(schema)
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


