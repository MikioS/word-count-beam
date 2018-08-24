package org.apache.beam.examples.complete.game;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TrackingAdMap {
	static TrackingAdBQ TrackingAdToTrackingAdBQ(TrackingAd trackingAd) {
		TrackingAdBQ trackingAdBQ = new TrackingAdBQ();
		trackingAdBQ.uuid = trackingAd.uuid;
		trackingAdBQ.time = trackingAd.time;
		trackingAdBQ.user_id = trackingAd.user.service_user_id;
		trackingAdBQ.client_ad_id = trackingAd.client.ad_id;
		trackingAdBQ.os = trackingAd.client.os;
		trackingAdBQ.ip = trackingAd.client.ip;
		trackingAdBQ.log_type = trackingAd.contents.log_type;
		trackingAdBQ.tracking_timing = trackingAd.contents.tracking_timing;
		trackingAdBQ.cue_point_id = trackingAd.contents.cue_point_id;
		trackingAdBQ.cluster_cf_id = trackingAd.contents.cluster_cf_id;
		trackingAdBQ.cluster_id = trackingAd.contents.cluster_id;
		trackingAdBQ.segment_group_id = trackingAd.contents.segment_group_id;
		trackingAdBQ.cue_point_sequence = trackingAd.contents.cue_point_sequence;
		trackingAdBQ.inserted_second = trackingAd.contents.inserted_second;
		trackingAdBQ.inserted_start_datetime = trackingAd.contents.inserted_start_datetime;
		trackingAdBQ.inserted_end_datetime = trackingAd.contents.inserted_end_datetime;
		trackingAdBQ.slot_id = trackingAd.contents.slot_id;
		trackingAdBQ.channel_id = trackingAd.contents.channel_id;
		trackingAdBQ.program_id = trackingAd.contents.program_id;
		trackingAdBQ.content_provider_id = trackingAd.contents.content_provider_id;
		trackingAdBQ.placement_method = trackingAd.contents.placement_method;
		trackingAdBQ.placement_job_id = trackingAd.contents.placement_job_id;
		trackingAdBQ.token_id = trackingAd.contents.token_id;
		trackingAdBQ.account_id = trackingAd.contents.account_id;
		trackingAdBQ.account_type = trackingAd.contents.account_type;
		trackingAdBQ.campaign_id = trackingAd.contents.campaign_id;
		trackingAdBQ.ad_id = trackingAd.contents.ad_id;
		trackingAdBQ.creative_id = trackingAd.contents.creative_id;
		trackingAdBQ.creative_asset_id = trackingAd.contents.creative_asset_id;
		trackingAdBQ.agency_id = trackingAd.contents.agency_id;
		trackingAdBQ.advertiser_brand_id = trackingAd.contents.advertiser_brand_id;
		trackingAdBQ.business_id = trackingAd.contents.business_id;
		trackingAdBQ.appeal_id = trackingAd.contents.appeal_id;
		trackingAdBQ.goal_id = trackingAd.contents.goal_id;
		trackingAdBQ.cm_code = trackingAd.contents.cm_code;
		trackingAdBQ.promo_type = trackingAd.contents.promo_type;
		trackingAdBQ.platform = trackingAd.platform;
		trackingAdBQ.device_model = trackingAd.client.device_model;
		trackingAdBQ.os_version = trackingAd.client.os_version;
		trackingAdBQ.app_version = trackingAd.client.app_version;
		trackingAdBQ.useragent = trackingAd.client.useragent;
		trackingAdBQ.url = trackingAd.page.url;
		trackingAdBQ.portrait = trackingAd.contents.portrait;
		trackingAdBQ.position = trackingAd.contents.position;
		return trackingAdBQ;

	
	}

	static TrackingAdBQ JsonToTrackingAd(String json) {
		ObjectMapper mapper = new ObjectMapper();

		JsonNode node;
		TrackingAdBQ trackingAd = new TrackingAdBQ();
		try {
			node = mapper.readTree(json);
			trackingAd.uuid = node.get("uuid").asText();
			trackingAd.time = node.get("time").asText();
			trackingAd.user_id = node.get("user").get("service_user_id").asText();
			trackingAd.client_ad_id = node.get("client").get("ad_id").asText();
			trackingAd.os = node.get("client").get("os").asText();
			trackingAd.ip = node.get("client").get("ip").asText();
			trackingAd.log_type = node.get("contents").get("log_type").asInt();
			trackingAd.tracking_timing = node.get("contents").get("tracking_timing").asInt();
			trackingAd.cue_point_id = node.get("contents").get("cue_point_id").asLong();
			trackingAd.cluster_cf_id = node.get("contents").get("cluster_cf_id").asInt();
			trackingAd.cluster_id = node.get("contents").get("cluster_id").asInt();
			trackingAd.segment_group_id = node.get("contents").get("segment_group_id").asText();
			trackingAd.cue_point_sequence = node.get("contents").get("cue_point_sequence").asInt();
			trackingAd.inserted_second = node.get("contents").get("inserted_second").asInt();
			trackingAd.inserted_start_datetime = node.get("contents").get("inserted_start_datetime").asLong();
			trackingAd.inserted_end_datetime = node.get("contents").get("inserted_end_datetime").asLong();
			trackingAd.slot_id = node.get("contents").get("slot_id").asText();
			trackingAd.channel_id = node.get("contents").get("channel_id").asText();
			trackingAd.program_id = node.get("contents").get("program_id").asText();
			trackingAd.content_provider_id = node.get("contents").get("content_provider_id").asText();
			trackingAd.placement_method = node.get("contents").get("placement_method").asLong();
			trackingAd.placement_job_id = node.get("contents").get("placement_job_id").asText();
			trackingAd.token_id = node.get("contents").get("token_id").asText();
			trackingAd.account_id = node.get("contents").get("account_id").asLong();
			trackingAd.account_type = node.get("contents").get("account_type").asInt();                
			trackingAd.campaign_id = node.get("contents").get("campaign_id").asLong();
			trackingAd.ad_id = node.get("contents").get("ad_id").asLong();
			trackingAd.creative_id = node.get("contents").get("creative_id").asLong();
			trackingAd.creative_asset_id = node.get("contents").get("creative_asset_id").asLong();
			trackingAd.agency_id = node.get("contents").get("agency_id").asLong();
			trackingAd.advertiser_brand_id = node.get("contents").get("advertiser_brand_id").asLong();
			trackingAd.business_id = node.get("contents").get("business_id").asLong();
			trackingAd.appeal_id = node.get("contents").get("appeal_id").asLong();
			trackingAd.goal_id = node.get("contents").get("goal_id").asLong();
			trackingAd.cm_code = node.get("contents").get("cm_code").asText();
			trackingAd.promo_type = node.get("contents").get("promo_type").asInt();
			trackingAd.platform = node.get("platform").asText();
			trackingAd.device_model = node.get("client").get("device_model").asText();
			trackingAd.os_version = node.get("client").get("os_version").asText();
			trackingAd.app_version = node.get("client").get("app_version").asText();
			trackingAd.useragent = node.get("client").get("useragent").asText();
			trackingAd.url = node.get("page").get("url").asText();
			trackingAd.portrait = node.get("contents").get("portrait").asBoolean();
			trackingAd.position = node.get("contents").get("position").asInt();

		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		return trackingAd;
		
	}

}
