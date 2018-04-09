package org.apache.beam.examples.complete.game;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.NullValue;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;


public class PubsubToBq extends HourlyTeamScore {

    interface Options extends HourlyTeamScore.Options, ExampleOptions, StreamingOptions {

        @Description("BigQuery Dataset to write tables to. Must already exist.")
        @Validation.Required
        String getDataset();
        void setDataset(String value);

        @Description("Pub/Sub topic to read from")
        @Validation.Required
        String getTopic();
        void setTopic(String value);

        @Description("Numeric value of fixed window duration for team analysis, in minutes")
        @Default.Integer(60)
        Integer getTeamWindowDuration();
        void setTeamWindowDuration(Integer value);

        @Description("Numeric value of allowed data lateness, in minutes")
        @Default.Integer(120)
        Integer getAllowedLateness();
        void setAllowedLateness(Integer value);

        @Description("Prefix used for the BigQuery table names")
        @Default.String("leaderboard")
        String getLeaderBoardTableName();
        void setLeaderBoardTableName(String value);
    }

    @DefaultCoder(AvroCoder.class)
    static class TrackingAd {
        @Nullable Integer time;
        @Nullable String user_id;
        @Nullable String client_ad_id;
/*
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
        @Nullable Integer placement_job_id;
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
*/

        public TrackingAd() {}

        public TrackingAd(Integer time, String user_id, String client_ad_id) {
            this.time = time;
            this.user_id = user_id;
            this.client_ad_id = client_ad_id;
        }

        public Integer getTime() {
            return this.time;
        }
        public String getUser_id() {
            return this.user_id;
        }
        public String getClient_ad_id() {
            return this.client_ad_id;
        }

    }

    static class ParseTrackingAdFn extends DoFn<String, TrackingAd> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            // Input(json) -> TrackingAd
            ObjectMapper mapper = new ObjectMapper();
            TrackingAd trackingAd = mapper.readValue(c.element(), TrackingAd.class);
            c.output(trackingAd);
        }
    }

    // TrackingAd -> TableRow
    static class MyFormatFunction implements SerializableFunction<TrackingAd, TableRow> {
        @Override
        public TableRow apply(TrackingAd trackingAd) {
            TableRow tableRow = new TableRow();
            tableRow.set("time", trackingAd.getTime());
            tableRow.set("user_id", trackingAd.getUser_id());
            tableRow.set("client_ad_id", trackingAd.getClient_ad_id());
            return tableRow;
        }
    }


    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        options.setStreaming(true); // true, falseは意味がない？
        Pipeline pipeline = Pipeline.create(options);

        PCollection<TrackingAd> trackingAds = pipeline
                .apply(PubsubIO.readStrings()
                    .fromTopic(options.getTopic()))
//                        .fromSubscription("projects/vega-177606/subscriptions/ebisawa_test"))
                .apply("ParseTrackingAd", ParDo.of(new ParseTrackingAdFn())); // CSV -> PCollection<TrackingAd>

        trackingAds
                .apply(
                        "WriteToBigQuery",
                        BigQueryIO
                        .<TrackingAd>write()
                        .to(options.getOutput())
                        .withFormatFunction(new MyFormatFunction())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) // ターゲットテーブルが存在しない場合、書き込み失敗
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)); // 末尾に追加


        pipeline.run();
        //pipeline.run().waitUntilFinish(); // <- バッチにならない

    }


}
