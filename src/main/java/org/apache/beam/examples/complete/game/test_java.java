package org.apache.beam.examples.complete.game;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.examples.complete.game.TrackingAd;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.joda.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;

public class test_java {
    public static void main(String[] args) {
    	Map<String, String> map = new HashMap<String, String>() {
    	    {
    	        put("key1","value1");
    	        put("key2","value2");
    	    }
    	};
    	
    	String aaa = map.get("key1");
    	System.out.println("aaa:" + aaa);
    	
    	String subscription = "projects/vega-177606/subscriptions/dev-test-ito-pubsub-to-gcs";
        String jsondata = "{\"schema\":\"tracking_ad/1-0-0\",\"uuid\":\"001\",\"time\":\"2018-04-18T19:05:07.140+09:00\",\"mine_id\":\"JfOswiat\",\"action_type\":\"tracking_ad\",\"platform\":\"native\",\"client\":{\"device_model\":\"abc\",\"ad_id\":\"idfagaid\",\"os\":\"iOS\",\"os_version\":\"11.2\",\"ip\":\"127.0.0.1\",\"useragent\":\"useragent dayo\",\"app_version\":\"app1\"},\"page\":{\"url\":\"url\"},\"user\":{\"attribute\":{},\"active_user\":true,\"service_user_id\":\"abema1\"},\"contents\":{\"log_type\":1,\"tracking_timing\":2,\"cue_point_id\":123,\"cluster_cf_id\":10,\"cluster_id\":0,\"cue_point_sequence\":2,\"inserted_second\":30,\"inserted_start_datetime\":1520169829,\"inserted_end_datetime\":1520171829,\"slot_id\":\"testSlotId\",\"channel_id\":\"testChannelId\",\"program_id\":\"testProgramId\",\"content_provider_id\":\"testContentProviderId\",\"placement_method\":123,\"placement_job_id\":\"testPlacementJobId\",\"token_id\":\"341z0rgq8n\",\"account_id\":3333,\"account_type\":1,\"campaign_id\":4444,\"ad_id\":5555,\"creative_id\":6666,\"creative_asset_id\":7777,\"agency_id\":1111,\"advertiser_brand_id\":2222,\"business_id\":9999,\"appeal_id\":1111111,\"goal_id\":8888,\"segment_group_id\":\"seg1,seg2,seg3\",\"portrait\":false,\"background_cluster_cf_id\":40,\"failed_times\":0,\"position\":10,\"cm_code\":\"test=12345\",\"promo_type\":2}}";

        try {
            ObjectMapper mapper = new ObjectMapper();
            TrackingAd trackingAd = mapper.readValue(jsondata, TrackingAd.class);
            System.out.println("uuid:"+ trackingAd.uuid);
        } catch (IOException e) {
            e.printStackTrace();
        }

    	
    }


}
