package org.apache.beam.examples.complete.game;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@DefaultCoder(AvroCoder.class)
@JsonIgnoreProperties(ignoreUnknown=true)
class TrackingAdBQ
{
	@Nullable public String uuid;
	@Nullable public String time;		
	@Nullable public String user_id;
	@Nullable public String client_ad_id;
	@Nullable public String os;
	@Nullable public String ip;
	@Nullable public Integer log_type;
	@Nullable public Integer tracking_timing;
	@Nullable public Long cue_point_id;
	@Nullable public Integer cluster_cf_id;
	@Nullable public Integer cluster_id;
	@Nullable public String segment_group_id;
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
	@Nullable public String cm_code;
	@Nullable public Integer promo_type;
	@Nullable public String platform;
	@Nullable public String device_model;
	@Nullable public String os_version;
	@Nullable public String app_version;
	@Nullable public String useragent;
	@Nullable public String url;
	@Nullable public Boolean portrait;
	@Nullable public Integer position;
}
