with
  ranking_cte AS (
    SELECT
      *,
      row_number() over (
        partition by
          ad_id,
          start_time
        order by
          processed_date desc,
          ad_id desc,
          adsquad_id desc
      ) as rank
    FROM
      "amd_iceberg"."amd_social"."social_snapchat_performance_ads" AS src
      INNER JOIN (
        SELECT distinct
          adaccountid
        FROM
          "amd_iceberg"."perf_admin"."pdn_ols_distribution_mapping"
        WHERE
          lower(replace(trim(platformsimple), ' ', '')) = 'snapchat'
          and omnilayoutguid = 'c55c1562-fe53-42e8-9109-7ce2a40de371'
      ) AS pe ON src.adaccount = pe.adaccountid
    WHERE
      ad_id IS NOT NULL
      AND start_time IS NOT NULL
  ),cte2 as 
  (
SELECT
  "impressions",
  "swipes",
  "quartile_1",
  "quartile_2",
  "quartile_3",
  "view_completion",
  "attachment_total_view_time_millis",
  "frequency",
  "swipe_up_percent",
  "attachment_frequency",
  "uniques",
  "attachment_uniques",
  (cast("spend" as double) / cast(1000000 as double)) as "spend",
  "video_views",
  "dma",
  "screen_time_millis",
  "shares",
  "ios_installs",
  "android_installs",
  "total_installs",
  "conversion_purchases",
  "conversion_purchases_value",
  "conversion_save",
  "conversion_start_checkout",
  "conversion_add_cart",
  "conversion_view_content",
  "conversion_add_billing",
  "conversion_sign_ups",
  "conversion_searches",
  "conversion_level_completes",
  "conversion_app_opens",
  "conversion_page_views",
  "conversion_subscribe",
  "conversion_ad_click",
  "conversion_ad_view",
  "conversion_complete_tutorial",
  "conversion_invite",
  "conversion_login",
  "conversion_share",
  "conversion_reserve",
  "conversion_achievement_unlocked",
  "conversion_add_to_wishlist",
  "conversion_spend_credits",
  "conversion_rate",
  "conversion_start_trial",
  "conversion_list_view",
  "custom_event_1",
  "custom_event_2",
  "custom_event_3",
  "custom_event_4",
  "custom_event_5",
  "ad_id",
  "adsquad_id",
  "granularity",
  "start_time",
  "end_time",
  "organization",
  "campaign_id",
  "processed_date",
  "report_date",
  "adaccount",
  current_date as edt_processed_date
FROM
  ranking_cte
WHERE
  rank = 1
  )
    select sum(swipes) as total_swipes,campaign_id from cte2
  group by campaign_id
  having campaign_id='968c4e7c-3412-4a88-a9ff-a0e3e31f98f3'---495
