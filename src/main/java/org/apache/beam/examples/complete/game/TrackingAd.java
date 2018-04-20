package org.apache.beam.examples.complete.game;

import org.apache.avro.reflect.Nullable;
import org.apache.avro.JsonProperties;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@DefaultCoder(AvroCoder.class)
@JsonIgnoreProperties(ignoreUnknown=true)

class TrackingAd
{

	@Nullable public String uuid;
	@Nullable public String time;		
	@Nullable public String action_type;
	@Nullable public String platform;
	@Nullable public client client;
	@Nullable public page page;
	@Nullable public user user;
	@Nullable public contents contents;

    @JsonIgnoreProperties(ignoreUnknown=true)
	static class client{
		@Nullable public String device_model;
		@Nullable public String ad_id;
		@Nullable public String os;
		@Nullable public String os_version;
		@Nullable public String ip;
		@Nullable public String useragent;
		@Nullable public String app_version;
		}

    @JsonIgnoreProperties(ignoreUnknown=true)
	static class page{
		@Nullable public String url;
	}

    @JsonIgnoreProperties(ignoreUnknown=true)
	static class user{
		@Nullable public String service_user_id;
	}
    @JsonIgnoreProperties(ignoreUnknown=true)
	static class contents{
		@Nullable public Integer log_type;
		@Nullable public Integer tracking_timing;
		@Nullable public Long cue_point_id;
		@Nullable public Integer cluster_cf_id;
		@Nullable public Integer cluster_id;
		@Nullable public Integer cue_point_sequence;
		@Nullable public Integer inserted_second;
		@Nullable public Long inserted_start_datetime;
		@Nullable public Long inserted_end_datetime;
		@Nullable public String slot_id;
		@Nullable public String channel_id;
		@Nullable public String program_id;
		@Nullable public String content_provider_id;
		@Nullable public Long placement_method;
		@Nullable public String placement_job_id;
		@Nullable public String token_id;
		@Nullable public Long account_id;
		@Nullable public Integer account_type;
		@Nullable public Long campaign_id;
		@Nullable public Long ad_id;
		@Nullable public Long creative_id;
		@Nullable public Long creative_asset_id;
		@Nullable public Long agency_id;
		@Nullable public Long advertiser_brand_id;
		@Nullable public Long business_id;
		@Nullable public Long appeal_id;
		@Nullable public Long goal_id;
		@Nullable public String segment_group_id;
		@Nullable public Boolean portrait;
		@Nullable public Integer background_cluster_cf_id;
		@Nullable public Integer failed_times;
		@Nullable public Integer position;
		@Nullable public String cm_code;
		@Nullable public Integer promo_type;
	}
}

/*
@DefaultCoder(AvroCoder.class)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TrackingAd
{

	@Nullable public String uuid;
	@Nullable public String time;	
	@Nullable public String action_type;
	@Nullable public String platform;
	@Nullable public client client;
	@Nullable public page page;
	@Nullable public user user;
	@Nullable public contents contents;

    @JsonIgnoreProperties(ignoreUnknown=true)
	static class client{
		@Nullable public String device_model;
		@Nullable public String ad_id;
		@Nullable public String os;
		@Nullable public String os_version;
		@Nullable public String ip;
		@Nullable public String useragent;
		@Nullable public String app_version;
		}

    @JsonIgnoreProperties(ignoreUnknown=true)
	static class page{
		@Nullable public String url;
	}

    @JsonIgnoreProperties(ignoreUnknown=true)
	static class user{
		@Nullable public String service_user_id;
	}
    @JsonIgnoreProperties(ignoreUnknown=true)
	static class contents{
		@Nullable public Integer log_type;
		@Nullable public Integer tracking_timing;
		@Nullable public Long cue_point_id;
		@Nullable public Integer cluster_cf_id;
		@Nullable public Integer cluster_id;
		@Nullable public Integer cue_point_sequence;
		@Nullable public Integer inserted_second;
		@Nullable public Long inserted_start_datetime;
		@Nullable public Long inserted_end_datetime;
		@Nullable public String slot_id;
		@Nullable public String channel_id;
		@Nullable public String program_id;
		@Nullable public String content_provider_id;
		@Nullable public Long placement_method;
		@Nullable public String placement_job_id;
		@Nullable public String token_id;
		@Nullable public Long account_id;
		@Nullable public Integer account_type;
		@Nullable public Long campaign_id;
		@Nullable public Long ad_id;
		@Nullable public Long creative_id;
		@Nullable public Long creative_asset_id;
		@Nullable public Long agency_id;
		@Nullable public Long advertiser_brand_id;
		@Nullable public Long business_id;
		@Nullable public Long appeal_id;
		@Nullable public Long goal_id;
		@Nullable public String segment_group_id;
		@Nullable public Boolean portrait;
		@Nullable public Integer background_cluster_cf_id;
		@Nullable public Integer failed_times;
		@Nullable public Integer position;
		@Nullable public String cm_code;
		@Nullable public Integer promo_type;
	}
}
*/