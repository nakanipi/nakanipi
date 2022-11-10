-- definition of test name, OS and test date
create temporary function get_test_name() as (['TWO_10151_metadata_search']);
create temporary function target_os() as (['ios', 'android']); 
create temporary function target_os_for_laplace() as (['DEVICE_IOS', 'DEVICE_ANDROID']); 
create temporary function target_os_for_search_api() as (['PLATFORM_IOS', 'PLATFORM_ANDROID']); 
create temporary function get_test_assign_start_time_jst() as (timestamp '2022-07-20 19:00:00');
create temporary function get_test_assign_end_time_jst() as (timestamp_sub(timestamp(current_date('Asia/Tokyo')), interval 1 second));
create temporary function get_test_start_time_jst() as (timestamp '2022-07-20 19:00:00');
create temporary function get_test_end_time_jst() as (timestamp_sub(timestamp(current_date('Asia/Tokyo')), interval 1 second));
create temporary function get_test_assign_start_date_utc() as (date(timestamp_sub(get_test_assign_start_time_jst(), interval 9 hour)));
create temporary function get_test_assign_end_date_utc() as (date(timestamp_sub(get_test_assign_end_time_jst(), interval 9 hour)));
create temporary function get_test_variant() as ([1, 2, 3, 4, 5]);

-- definition of pascal event
create temporary function is_launch_event(c string, i string) as (c = 'APP_LAUNCH' and i = '');
create temporary function is_item_detail_display_event(c string, i string) as (c = 'ITEM_VIEW' and i = 'item_details'); --多分
create temporary function is_item_like_tap_event(c string, i string) as (c = 'ITEM_LIKE' and i = 'item_details:item_info'); 
create temporary function is_buy_complete_event(c string, i string) as (c = 'PURCHASE_COMPLETED' and i = ''); --購買発生 
--create temporary function is_shops_item_detail_display_event(c string, i string) as (c = 'shops_item' and i = 'shops_item_detail_display'); --shops
--create temporary function is_shops_item_like_tap_event(c string, i string) as (c = 'shops_item' and i = 'shops_item_like_tap'); --shops / いいねは未実装
--create temporary function is_shops_buy_complete_event(c string, i string) as (c = 'shops_buy' and i = 'shops_buy_complete'); --shops
create temporary function is_search_execute_event(c string, i string) as (c = 'SEARCH' and i = '');
create temporary function is_search_scroll_event(c string, i string) as (c = 'SCROLL' and i = 'search_result:<tab>:body:items_list');
--create temporary function is_sell_event(c string, i string) as (c = 'sell' and i = 'sell_input_list_complete'); --多分ないので他から補完すべき

-- definition of official user
create temporary function is_official_user(user_id int64) returns bool as (
  (user_id is not null) and (user_id in (select user_id from `kouzoh-analytics-jp-prod.intermediates.v_abtest_ineligible_users`))
);

-- calculation of var using delta method see: https://arxiv.org/pdf/1803.06336.pdf
create temporary function calc_var_with_delta(n_sample int64, numerator_avg float64, numerator_var float64, denominator_avg float64, denominator_var float64, numerator_denominator_covar float64) as (
  safe_divide(1, n_sample * pow(denominator_avg, 2)) * (numerator_var - 2 * safe_divide(numerator_avg, denominator_avg) * numerator_denominator_covar + pow(safe_divide(numerator_avg, denominator_avg), 2) * denominator_var)
);

-- extract test target user
with user_assignments as (
  select
    user_id
    , case
        when client.platform = 'PLATFORM_IOS' then 'ios'
        when client.platform = 'PLATFORM_ANDROID' then 'android'
        else 'other'
        end as type
    , max(e.variant) as v
    , min(receive_time) as assign_time
  from
    `kouzoh-analytics-jp-prod.mercari_search_adapter_jp.mercari_platform_event_searchadapter_v2_searcheventlog` as t
    , unnest(experiments) as e
  where
    date(_PARTITIONTIME) between get_test_assign_start_date_utc() and get_test_assign_end_date_utc() -- SA log PARTITION is UTC
    and e.variant in unnest(get_test_variant())
    and e.name in unnest(get_test_name())
    and timestamp_add(receive_time, interval 9 hour) between get_test_assign_start_time_jst() and get_test_assign_end_time_jst()
    and not is_official_user(user_id)
    and client.platform in unnest(target_os_for_search_api())
  group by 1, 2
)

-- target event
, activity_c as (
  select
    ctime
    , context.user_id
    , event.type as event_type
    , event.location as event_location
    , event.value as value_c
    --, json_extract_scalar(prop, "$.item_id") item_id

    --, json_extract_scalar(prop, "$.category") category_id
    --, json_extract_scalar(prop, "$.order_id") order_id
    , context.type
  from
    `kouzoh-analytics-jp-prod.intermediates.pascal_event_log`
  where
    _PARTITIONDATE between date(get_test_start_time_jst()) and date(get_test_end_time_jst()) -- pascal PARTITION is JST
    and (is_launch_event(event.type, event.location) --APP
      or is_search_execute_event(event.type, event.location)
      or is_search_scroll_event(event.type, event.location)
      or is_item_detail_display_event(event.type, event.location)
      or is_item_like_tap_event(event.type, event.location)
      or is_buy_complete_event(event.type, event.location)
      or is_sell_event(event.type, event.location)
      --or is_shops_item_detail_display_event(event.type, event.location)
      --or is_shops_buy_complete_event(event.type, event.location)
    )
    and type in unnest(target_os())
)

-- item category name
, item_categories as (
  select
    cast(category_id as string) as category_id,
    category_level1_name, 
    category_level2_name,
  from 
    `kouzoh-analytics-jp-prod.components_listing_and_buying.item_categories`
)

, shops_product_with_category as (
  select 
    product_id
    , created_at
    , category_id as shops_category_id
    , lead(created_at, 1) over (partition by product_id order by created_at) as created_at_next
    , lead(category_id, 1) over (partition by product_id order by created_at) as category_id_next
    , max(created_at) over (partition by product_id order by created_at rows between unbounded preceding and unbounded following) as created_at_max
  from 
    `mercari-search-dwh-jp-prod.souzoh_beyond_jp.view__product_to_categories`
    , unnest(category_ids)
)

-- add price of Shops items (value field sends only price of Mercari items)
-- add category_name
, activity as (
  select
    activity_c.*
    , total_price as value_b
    , if(
      is_item_detail_display_event(event_type, event_location) or is_item_like_tap_event(event_type, event_location) or is_buy_complete_event(event_type, event_location)
      , category_level1_name
        --, if(
        --  is_shops_item_detail_display_event(event_type, event_location) or is_shops_item_like_tap_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location),
        --  category_level1_name_shops,
          null
        --)
    ) as category_level1_name
    , if(
      is_item_detail_display_event(event_type, event_location) or is_item_like_tap_event(event_type, event_location) or is_buy_complete_event(event_type, event_location)
      , category_level2_name
      --, if(
      --  is_shops_item_detail_display_event(event_type, event_location) or is_shops_item_like_tap_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)
      --  , category_level2_name_shops
         null
      --)
    ) as category_level2_name
  from activity_c
   -- add price of Shops items
    left join (
      select
        id as order_id
        , total_price
      from
        `kouzoh-analytics-jp-prod.souzoh_beyond_jp_order.orders`
    ) shops_item
      using(order_id)

    -- add item categories of Mercari items
    left join item_categories
      using(category_id)

    -- add item categories of Shops items
    left join shops_product_with_category as spc
      on activity_c.item_id = spc.product_id
        and (
             (ctime < created_at_next and ctime >= created_at)
          or (created_at_max <= ctime and  created_at_max = created_at)
        )
    left outer join (
      select distinct 
        id,
        mercari_category_id,
      from `kouzoh-analytics-jp-prod.souzoh_beyond_jp_productcategory.product_category_masters`
      ) as pcm on spc.shops_category_id = pcm.id
    left outer join (
      select 
        category_id,
        category_level1_name as category_level1_name_shops,
        category_level2_name as category_level2_name_shops
    from 
        `kouzoh-analytics-jp-prod.components_listing_and_buying.item_categories`
    ) as ic on pcm.mercari_category_id = ic.category_id
)

-- test users' activity
, test_user_activity as (
  select
    assign_time
    , v
    , activity.*
  from
    user_assignments 
    left join activity
      using(user_id, type)
  where
    user_assignments.assign_time < activity.ctime
    and timestamp_add(activity.ctime, interval 9 hour) between get_test_start_time_jst() and get_test_end_time_jst()
)

, first_view AS (
    WITH
    view_time AS (
        SELECT *
        FROM `mercari-search-dwh-jp-prod.item_detail_display.first_touch_timestamp_parted`
        WHERE
                (TIMESTAMP_ADD(ctime, INTERVAL 9 HOUR) BETWEEN get_test_start_time_jst() AND get_test_end_time_jst())
            AND (type IN UNNEST(target_os()))
    ),
    _events AS (
        SELECT
             timestamp_add(context.client_time, interval 9 hour) as ctime
            ,timestamp_add(context.server_time, interval 9 hour) as stime
            , context.user_id
            , context.type
            , event.item_id AS item_id
            , COALESCE(
                  -- search_sort in ("score", "created_time", "num_likes", "price")
                  is_search_sort_high_price(event.type, event.location)
                  , is_search_sort_low_price(event.type, event.location)
                  , is_search_sort_new(event.type, event.location)
                  , is_search_sort_like(event.type, event.location)
                --, JSON_EXTRACT_SCALAR(prop, '$.search_conditions.sort')
              ) AS search_sort
            /*, COALESCE(
                --たぶんキーワードしか取れないのでsearch_adapterから取るしかないね
                --[filter , hash_tag, history, intent, keyword, keyword_on_search_result, realtime_notification, shallow_facet, save]
                --is_search_type(event.type, event.location)
              ) AS search_type
            , COALESCE(
                --これも差分取らないととれない、numbさんが作ってくれている？
                --  JSON_EXTRACT_SCALAR(prop, '$.source')
                --, JSON_EXTRACT_SCALAR(prop, '$.search_conditions.source')
              ) AS source
            */
        FROM `mercari-data-infra-prod.events.client_events_v2`
        WHERE
                (date(server_time, 'Asia/Tokyo') between date(get_test_start_time_jst()) and date(get_test_end_time_jst())) -- pascal PARTITION is JST
            AND (
                   is_item_detail_display_event(event.type, event.location)
                --OR is_shops_item_detail_display_event(event_type, event_location)
            )
            AND (context.type IN UNNEST(target_os()))
    ),
    _fv AS (
        SELECT
              ctime
            , user_id
            , item_id
            , ANY_VALUE(STRUCT(
                  search_sort
                , search_type
                , source
            )) AS search
        FROM _events INNER JOIN view_time USING(ctime, stime, user_id, type, item_id)
        GROUP BY 1,2,3
    )
    SELECT
          ctime
        , user_id
        , item_id
        , search.search_sort
        , search.search_type
        , search.source
    FROM _fv
)

-- aggregation of target activities
, user_act_summary as (
  select
    user_id
    , v
    , type
    , count(distinct date(test_user_activity.ctime, 'Asia/Tokyo')) n_visit
    , countif(is_search_execute_event(event_type, event_location)) n_search
    , countif(is_search_scroll_event(event_type, event_location)) n_scroll

    -- view
    , countif(is_item_detail_display_event(event_type, event_location) or is_shops_item_detail_display_event(event_type, event_location)) n_view
    , countif(is_item_detail_display_event(event_type, event_location)) n_view_c
    --, countif(is_shops_item_detail_display_event(event_type, event_location)) n_view_b
    --, countif((is_item_detail_display_event(event_type, event_location) or is_shops_item_detail_display_event(event_type, event_location)) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_view_via_search
    , countif(is_item_detail_display_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_view_via_search_c
    --, countif(is_shops_item_detail_display_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_view_via_search_b
    --, countif((is_item_detail_display_event(event_type, event_location) or is_shops_item_detail_display_event(event_type, event_location)) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_view_via_search_best_match
    , countif(is_item_detail_display_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_view_via_search_best_match_c
    --, countif(is_shops_item_detail_display_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_view_via_search_best_match_b
    
    -- like
    --, countif(is_item_like_tap_event(event_type, event_location) or is_shops_item_like_tap_event(event_type, event_location)) n_like
    , countif(is_item_like_tap_event(event_type, event_location)) n_like_c
    --, countif(is_shops_item_like_tap_event(event_type, event_location)) n_like_b
    --, countif((is_item_like_tap_event(event_type, event_location) or is_shops_item_like_tap_event(event_type, event_location)) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_like_via_search
    , countif(is_item_like_tap_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_like_via_search_c
    --, countif(is_shops_item_like_tap_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_like_via_search_b
    --, countif((is_item_like_tap_event(event_type, event_location) or is_shops_item_like_tap_event(event_type, event_location)) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_like_via_search_best_match
    , countif(is_item_like_tap_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_like_via_search_best_match_c
    --, countif(is_shops_item_like_tap_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_like_via_search_best_match_b
    
    -- bcr
    --, count(distinct if(is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr
    , count(distinct if(is_buy_complete_event(event_type, event_location), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_c
    --, count(distinct if(is_shops_buy_complete_event(event_type, event_location), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_b
    --, count(distinct if((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price"), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_via_search
    , count(distinct if(is_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price"), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_via_search_c
    --, count(distinct if(is_shops_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price"), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_via_search_b
    --, count(distinct if((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score"), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_via_search_best_match
    , count(distinct if(is_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score"), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_via_search_best_match_c
    --, count(distinct if(is_shops_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score"), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_via_search_best_match_b
    
    -- buy
    --, countif(is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) n_buy
    , countif(is_buy_complete_event(event_type, event_location)) n_buy_c
    --, countif(is_shops_buy_complete_event(event_type, event_location)) n_buy_b
    --, countif((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_buy_via_search
    , countif(is_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_buy_via_search_c
    --, countif(is_shops_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price")) n_buy_via_search_b
    --, countif((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_buy_via_search_best_match
    , countif(is_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_buy_via_search_best_match_c
    --, countif(is_shops_buy_complete_event(event_type, event_location) and  ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score")) n_buy_via_search_best_match_b

    -- gmv
    --, sum(if(is_buy_complete_event(event_type, event_location), value_c, 0) + if(is_shops_buy_complete_event(event_type, event_location), value_b, 0)) n_gmv
    , sum(if(is_buy_complete_event(event_type, event_location), value_c, 0)) n_gmv_c
    --, sum(if(is_shops_buy_complete_event(event_type, event_location), value_b, 0)) n_gmv_b
    --, sum(if(is_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price"), value_c, 0) + if(is_shops_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price"), value_b, 0)) n_gmv_via_search
    , sum(if(is_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price"), value_c, 0)) n_gmv_via_search_c
    --, sum(if(is_shops_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score", "created_time", "num_likes", "price"), value_b, 0)) n_gmv_via_search_b
    --, sum(if(is_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score"), value_c, 0) + if(is_shops_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score"), value_b, 0)) n_gmv_via_search_best_match
    , sum(if(is_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score"), value_c, 0)) n_gmv_via_search_best_match_c
    --, sum(if(is_shops_buy_complete_event(event_type, event_location) and ((search_type is not null and source <> "similar_on_item") or (source like "home_myList_component_100%")) and search_sort in ("score"), value_b, 0)) n_gmv_via_search_best_match_b

    , count(distinct if(is_sell_event(event_type, event_location), date(test_user_activity.ctime, 'Asia/Tokyo'), null)) lcr
    , countif(is_sell_event(event_type, event_location)) n_list

    -- by categories (handmaid)
    --, countif((is_item_detail_display_event(event_type, event_location) or is_shops_item_detail_display_event(event_type, event_location)) and category_level1_name = 'ハンドメイド') n_view_handmaid
    , countif(is_item_detail_display_event(event_type, event_location) and category_level1_name = 'ハンドメイド') n_view_handmaid_c
    --, countif(is_shops_item_detail_display_event(event_type, event_location) and category_level1_name = 'ハンドメイド') n_view_handmaid_b
    --, count(distinct if((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and category_level1_name = 'ハンドメイド', date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_handmaid
    , count(distinct if(is_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド', date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_handmaid_c
    --, count(distinct if(is_shops_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド', date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_handmaid_b
    --, countif((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and category_level1_name = 'ハンドメイド') n_buy_handmaid
    , countif(is_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド') n_buy_handmaid_c
    --, countif(is_shops_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド') n_buy_handmaid_b
    --, sum(if(is_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド', value_c, 0) + if(is_shops_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド', value_b, 0)) n_gmv_handmaid
    , sum(if(is_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド', value_c, 0)) n_gmv_handmaid_c
    --, sum(if(is_shops_buy_complete_event(event_type, event_location) and category_level1_name = 'ハンドメイド', value_b, 0)) n_gmv_handmaid_b

    -- by categories (food)
    --, countif((is_item_detail_display_event(event_type, event_location) or is_shops_item_detail_display_event(event_type, event_location)) and category_level2_name = '食品') n_view_food
    , countif(is_item_detail_display_event(event_type, event_location) and category_level2_name = '食品') n_view_food_c
    --, countif(is_shops_item_detail_display_event(event_type, event_location) and category_level2_name = '食品') n_view_food_b
    --, count(distinct if((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and category_level2_name = '食品', date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_food
    , count(distinct if(is_buy_complete_event(event_type, event_location) and category_level2_name = '食品', date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_food_c
    --, count(distinct if(is_shops_buy_complete_event(event_type, event_location) and category_level2_name = '食品', date(test_user_activity.ctime, 'Asia/Tokyo'), null)) bcr_food_b
    --, countif((is_buy_complete_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)) and category_level2_name = '食品') n_buy_food
    , countif(is_buy_complete_event(event_type, event_location) and category_level2_name = '食品') n_buy_food_c
    --, countif(is_shops_buy_complete_event(event_type, event_location) and category_level2_name = '食品') n_buy_food_b
    --, sum(if(is_buy_complete_event(event_type, event_location) and category_level2_name = '食品', value_c, 0) + if(is_shops_buy_complete_event(event_type, event_location) and category_level2_name = '食品', value_b, 0)) n_gmv_food
    , sum(if(is_buy_complete_event(event_type, event_location) and category_level2_name = '食品', value_c, 0)) n_gmv_food_c
    --, sum(if(is_shops_buy_complete_event(event_type, event_location) and category_level2_name = '食品', value_b, 0)) n_gmv_food_b
  from
    test_user_activity
    left join first_view
      using(user_id, item_id)
  where
    if(
      is_item_detail_display_event(event_type, event_location) or is_item_like_tap_event(event_type, event_location) or is_buy_complete_event(event_type, event_location)
        --or is_shops_item_detail_display_event(event_type, event_location) or is_shops_item_like_tap_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)
      , first_view.ctime > test_user_activity.assign_time
      , true
    ) -- a/bテスト後の閲覧・いいね・購入のものに絞り込み
  group by 1,2,3
)

-- calculation of seller uu
, seller_summary as (
  select
    v
    , type
    --, count(distinct if(is_buy_complete_event(event_type, event_location), seller_id, null)) + count(distinct if(is_shops_buy_complete_event(event_type, event_location), shop_id, null)) seller_uu
    , count(distinct if(is_buy_complete_event(event_type, event_location), seller_id, null)) seller_uu_c
    --, count(distinct if(is_shops_buy_complete_event(event_type, event_location), shop_id, null)) seller_uu_b
  from
    test_user_activity
    left join first_view
      using (user_id, item_id)
    left join (
      select
        id
        , seller_id
      from
        `mercari-anondb-jp-prod.anon_jp.items`
    ) as items on test_user_activity.item_id = items.id
    left join (
      select
        id
        , shop_id
      from
        `kouzoh-analytics-jp-prod.souzoh_beyond_jp_product.products`
    ) as shops_items on test_user_activity.item_id = shops_items.id
  where
    if(
        is_item_detail_display_event(event_type, event_location) or is_item_like_tap_event(event_type, event_location) or is_buy_complete_event(event_type, event_location)
          --or is_shops_item_detail_display_event(event_type, event_location) or is_shops_item_like_tap_event(event_type, event_location) or is_shops_buy_complete_event(event_type, event_location)
        , first_view.ctime > test_user_activity.assign_time
        , true)
  group by 1, 2
)

-- calculation of impressions
-- ここも置き換えるか確認 v2 たしかimpの集計に差異が出ていたはずなので
, imp_log as (
  select
    client_time as ctime
    , safe_cast(context.user.user_id as int64) as user_id
    , e.item.type as item_type
    , e.item.id as item_id
    , case
        when context.device.type = 'DEVICE_IOS' then 'ios'
        when context.device.type = 'DEVICE_ANDROID' then 'android'
      else 'other' end as type
  from 
    `mercari-data-infra-prod.events.client_events_v1` as t,
    t.event.impressions as e
  where
    timestamp_add(server_time, interval 9 hour) between get_test_start_time_jst() and get_test_end_time_jst()
    and event.id.type = 'ITEM_IMPRESSION'
    and context.device.type in unnest(target_os_for_laplace())
    and event.id.location.screen like 'search_result_tab_%'
)

, test_user_imp as (
  select 
    v
    , imp_log.*
  from 
    user_assignments
    left join imp_log
    using(user_id, type)
  where
    user_assignments.assign_time < imp_log.ctime
    and timestamp_add(imp_log.ctime, interval 9 hour) between get_test_start_time_jst() and get_test_end_time_jst()
)

, imp_summary as (
  select
    user_id
    , v
    , type
    , count(if(item_type = 'ITEM_TYPE_MERCARI' or item_type = 'ITEM_TYPE_BEYOND', item_id, null)) as n_imp
    , count(if(item_type = 'ITEM_TYPE_MERCARI', item_id, null)) as n_imp_c
    , count(if(item_type = 'ITEM_TYPE_BEYOND', item_id, null)) as n_imp_b
  from 
    test_user_imp
  group by 1, 2, 3
)

-- cがmercari
-- bがshops
, summary as (
  select
    v
    , type
    , count(distinct user_id) n_user
    , avg(n_visit) n_visit_day
    , safe_divide(avg(n_search), avg(n_visit)) avg_n_search
    , safe_divide(avg(n_scroll), avg(n_visit)) avg_n_scroll

    -- view
    --, safe_divide(avg(n_view), avg(n_visit)) avg_n_view
    , safe_divide(avg(n_view_c), avg(n_visit)) avg_n_view_c
    --, safe_divide(avg(n_view_b), avg(n_visit)) avg_n_view_b
    --, safe_divide(avg(n_view_via_search), avg(n_visit)) avg_n_view_via_search
    , safe_divide(avg(n_view_via_search_c), avg(n_visit)) avg_n_view_via_search_c
    --, safe_divide(avg(n_view_via_search_b), avg(n_visit)) avg_n_view_via_search_b
    --, safe_divide(avg(n_view_via_search_best_match), avg(n_visit)) avg_n_view_via_search_best_match
    , safe_divide(avg(n_view_via_search_best_match_c), avg(n_visit)) avg_n_view_via_search_best_match_c
    --, safe_divide(avg(n_view_via_search_best_match_b), avg(n_visit)) avg_n_view_via_search_best_match_b
    
    -- like
    --, safe_divide(avg(n_like), avg(n_visit)) avg_n_like
    , safe_divide(avg(n_like_c), avg(n_visit)) avg_n_like_c
    --, safe_divide(avg(n_like_b), avg(n_visit)) avg_n_like_b
    --, safe_divide(avg(n_like_via_search), avg(n_visit)) avg_n_like_via_search
    , safe_divide(avg(n_like_via_search_c), avg(n_visit)) avg_n_like_via_search_c
    --, safe_divide(avg(n_like_via_search_b), avg(n_visit)) avg_n_like_via_search_b
    --, safe_divide(avg(n_like_via_search_best_match), avg(n_visit)) avg_n_like_via_search_best_match
    , safe_divide(avg(n_like_via_search_best_match_c), avg(n_visit)) avg_n_like_via_search_best_match_c
    --, safe_divide(avg(n_like_via_search_best_match_b), avg(n_visit)) avg_n_like_via_search_best_match_b

    -- bcr
    --, safe_divide(avg(bcr), avg(n_visit)) avg_bcr
    , safe_divide(avg(bcr_c), avg(n_visit)) avg_bcr_c
    --, safe_divide(avg(bcr_b), avg(n_visit)) avg_bcr_b
    --, safe_divide(avg(bcr_via_search), avg(n_visit)) avg_bcr_via_search
    , safe_divide(avg(bcr_via_search_c), avg(n_visit)) avg_bcr_via_search_c
    --, safe_divide(avg(bcr_via_search_b), avg(n_visit)) avg_bcr_via_search_b
    --, safe_divide(avg(bcr_via_search_best_match), avg(n_visit)) avg_bcr_via_search_best_match
    , safe_divide(avg(bcr_via_search_best_match_c), avg(n_visit)) avg_bcr_via_search_best_match_c
    --, safe_divide(avg(bcr_via_search_best_match_b), avg(n_visit)) avg_bcr_via_search_best_match_b

    -- buy
    --, safe_divide(avg(n_buy), avg(n_visit)) avg_n_buy
    , safe_divide(avg(n_buy_c), avg(n_visit)) avg_n_buy_c
    --, safe_divide(avg(n_buy_b), avg(n_visit)) avg_n_buy_b
    --, safe_divide(avg(n_buy_via_search), avg(n_visit)) avg_n_buy_via_search
    , safe_divide(avg(n_buy_via_search_c), avg(n_visit)) avg_n_buy_via_search_c
    --, safe_divide(avg(n_buy_via_search_b), avg(n_visit)) avg_n_buy_via_search_b
    --, safe_divide(avg(n_buy_via_search_best_match), avg(n_visit)) avg_n_buy_via_search_best_match
    , safe_divide(avg(n_buy_via_search_best_match_c), avg(n_visit)) avg_n_buy_via_search_best_match_c
    --, safe_divide(avg(n_buy_via_search_best_match_b), avg(n_visit)) avg_n_buy_via_search_best_match_b

    -- gmv
    --, safe_divide(avg(n_gmv), avg(n_visit)) avg_n_gmv
    , safe_divide(avg(n_gmv_c), avg(n_visit)) avg_n_gmv_c
    --, safe_divide(avg(n_gmv_b), avg(n_visit)) avg_n_gmv_b
    --, safe_divide(avg(n_gmv_via_search), avg(n_visit)) avg_n_gmv_via_search
    , safe_divide(avg(n_gmv_via_search_c), avg(n_visit)) avg_n_gmv_via_search_c
    --, safe_divide(avg(n_gmv_via_search_b), avg(n_visit)) avg_n_gmv_via_search_b
    --, safe_divide(avg(n_gmv_via_search_best_match), avg(n_visit)) avg_n_gmv_via_search_best_match
    , safe_divide(avg(n_gmv_via_search_best_match_c), avg(n_visit)) avg_n_gmv_via_search_best_match_c
    --, safe_divide(avg(n_gmv_via_search_best_match_b), avg(n_visit)) avg_n_gmv_via_search_best_match_b

    -- aov
    --, safe_divide(avg(n_gmv), avg(n_buy)) avg_n_aov
    , safe_divide(avg(n_gmv_c), avg(n_buy_c)) avg_n_aov_c
    --, safe_divide(avg(n_gmv_b), avg(n_buy_b)) avg_n_aov_b
    --, safe_divide(avg(n_gmv_via_search), avg(n_buy_via_search)) avg_n_aov_via_search
    , safe_divide(avg(n_gmv_via_search_c), avg(n_buy_via_search_c)) avg_n_aov_via_search_c
    --, safe_divide(avg(n_gmv_via_search_b), avg(n_buy_via_search_b)) avg_n_aov_via_search_b
    --, safe_divide(avg(n_gmv_via_search_best_match), avg(n_buy_via_search_best_match)) avg_n_aov_via_search_best_match
    , safe_divide(avg(n_gmv_via_search_best_match_c), avg(n_buy_via_search_best_match_c)) avg_n_aov_via_search_best_match_c
    --, safe_divide(avg(n_gmv_via_search_best_match_b), avg(n_buy_via_search_best_match_b)) avg_n_aov_via_search_best_match_b

    -- list
    , safe_divide(avg(lcr), avg(n_visit)) avg_lcr
    , safe_divide(avg(n_list), avg(n_visit)) avg_n_list

    -- handmaid
    --, safe_divide(avg(n_view_handmaid), avg(n_visit)) avg_n_view_handmaid
    , safe_divide(avg(n_view_handmaid_c), avg(n_visit)) avg_n_view_c_handmaid
    --, safe_divide(avg(n_view_handmaid_b), avg(n_visit)) avg_n_view_b_handmaid
    --, safe_divide(avg(bcr_handmaid), avg(n_visit)) avg_bcr_handmaid
    , safe_divide(avg(bcr_handmaid_c), avg(n_visit)) avg_bcr_handmaid_c
    --, safe_divide(avg(bcr_handmaid_b), avg(n_visit)) avg_bcr_handmaid_b
    --, safe_divide(avg(n_buy_handmaid), avg(n_visit)) avg_n_buy_handmaid
    , safe_divide(avg(n_buy_handmaid_c), avg(n_visit)) avg_n_buy_handmaid_c
    --, safe_divide(avg(n_buy_handmaid_b), avg(n_visit)) avg_n_buy_handmaid_b
    --, safe_divide(avg(n_gmv_handmaid), avg(n_visit)) avg_n_gmv_handmaid
    , safe_divide(avg(n_gmv_handmaid_c), avg(n_visit)) avg_n_gmv_handmaid_c
    --, safe_divide(avg(n_gmv_handmaid_b), avg(n_visit)) avg_n_gmv_handmaid_b

    -- food
    --, safe_divide(avg(n_view_food), avg(n_visit)) avg_n_view_food
    , safe_divide(avg(n_view_food_c), avg(n_visit)) avg_n_view_c_food
    --, safe_divide(avg(n_view_food_b), avg(n_visit)) avg_n_view_b_food
    --, safe_divide(avg(bcr_food), avg(n_visit)) avg_bcr_food
    , safe_divide(avg(bcr_food_c), avg(n_visit)) avg_bcr_food_c
    --, safe_divide(avg(bcr_food_b), avg(n_visit)) avg_bcr_food_b
    --, safe_divide(avg(n_buy_food), avg(n_visit)) avg_n_buy_food
    , safe_divide(avg(n_buy_food_c), avg(n_visit)) avg_n_buy_food_c
    --, safe_divide(avg(n_buy_food_b), avg(n_visit)) avg_n_buy_food_b
    --, safe_divide(avg(n_gmv_food), avg(n_visit)) avg_n_gmv_food
    , safe_divide(avg(n_gmv_food_c), avg(n_visit)) avg_n_gmv_food_c
    --, safe_divide(avg(n_gmv_food_b), avg(n_visit)) avg_n_gmv_food_b

    -- imp
    --, safe_divide(avg(n_imp), avg(n_visit)) avg_n_imp
    , safe_divide(avg(n_imp_c), avg(n_visit)) avg_n_imp_c
    --, safe_divide(avg(n_imp_b), avg(n_visit)) avg_n_imp_b

    -- calculation of variance
    , var_samp(n_visit) / count(distinct user_id) var_n_visit_day
    , calc_var_with_delta(count(distinct user_id), avg(n_search), var_samp(n_search), avg(n_visit), var_samp(n_visit), covar_samp(n_search, n_visit)) as var_n_search
    , calc_var_with_delta(count(distinct user_id), avg(n_scroll), var_samp(n_scroll), avg(n_visit), var_samp(n_visit), covar_samp(n_scroll, n_visit)) as var_n_scroll
    
    -- view variance
    --, calc_var_with_delta(count(distinct user_id), avg(n_view), var_samp(n_view), avg(n_visit), var_samp(n_visit), covar_samp(n_view, n_visit)) as var_n_view
    , calc_var_with_delta(count(distinct user_id), avg(n_view_c), var_samp(n_view_c), avg(n_visit), var_samp(n_visit), covar_samp(n_view_c, n_visit)) as var_n_view_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_b), var_samp(n_view_b), avg(n_visit), var_samp(n_visit), covar_samp(n_view_b, n_visit)) as var_n_view_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_via_search), var_samp(n_view_via_search), avg(n_visit), var_samp(n_visit), covar_samp(n_view_via_search, n_visit)) as var_n_view_via_search
    , calc_var_with_delta(count(distinct user_id), avg(n_view_via_search_c), var_samp(n_view_via_search_c), avg(n_visit), var_samp(n_visit), covar_samp(n_view_via_search_c, n_visit)) as var_n_view_via_search_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_via_search_b), var_samp(n_view_via_search_b), avg(n_visit), var_samp(n_visit), covar_samp(n_view_via_search_b, n_visit)) as var_n_view_via_search_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_via_search_best_match), var_samp(n_view_via_search_best_match), avg(n_visit), var_samp(n_visit), covar_samp(n_view_via_search_best_match, n_visit)) as var_n_view_via_search_best_match
    , calc_var_with_delta(count(distinct user_id), avg(n_view_via_search_best_match_c), var_samp(n_view_via_search_best_match_c), avg(n_visit), var_samp(n_visit), covar_samp(n_view_via_search_best_match_c, n_visit)) as var_n_view_via_search_best_match_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_via_search_best_match_b), var_samp(n_view_via_search_best_match_b), avg(n_visit), var_samp(n_visit), covar_samp(n_view_via_search_best_match_b, n_visit)) as var_n_view_via_search_best_match_b
    
    -- like variance
    --, calc_var_with_delta(count(distinct user_id), avg(n_like), var_samp(n_like), avg(n_visit), var_samp(n_visit), covar_samp(n_like, n_visit)) as var_n_like
    , calc_var_with_delta(count(distinct user_id), avg(n_like_c), var_samp(n_like_c), avg(n_visit), var_samp(n_visit), covar_samp(n_like_c, n_visit)) as var_n_like_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_like_b), var_samp(n_like_b), avg(n_visit), var_samp(n_visit), covar_samp(n_like_b, n_visit)) as var_n_like_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_like_via_search), var_samp(n_like_via_search), avg(n_visit), var_samp(n_visit), covar_samp(n_like_via_search, n_visit)) as var_n_like_via_search
    , calc_var_with_delta(count(distinct user_id), avg(n_like_via_search_c), var_samp(n_like_via_search_c), avg(n_visit), var_samp(n_visit), covar_samp(n_like_via_search_c, n_visit)) as var_n_like_via_search_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_like_via_search_b), var_samp(n_like_via_search_b), avg(n_visit), var_samp(n_visit), covar_samp(n_like_via_search_b, n_visit)) as var_n_like_via_search_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_like_via_search_best_match), var_samp(n_like_via_search_best_match), avg(n_visit), var_samp(n_visit), covar_samp(n_like_via_search_best_match, n_visit)) as var_n_like_via_search_best_match
    , calc_var_with_delta(count(distinct user_id), avg(n_like_via_search_best_match_c), var_samp(n_like_via_search_best_match_c), avg(n_visit), var_samp(n_visit), covar_samp(n_like_via_search_best_match_c, n_visit)) as var_n_like_via_search_best_match_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_like_via_search_best_match_b), var_samp(n_like_via_search_best_match_b), avg(n_visit), var_samp(n_visit), covar_samp(n_like_via_search_best_match_b, n_visit)) as var_n_like_via_search_best_match_b
    
    -- bcr variance
    --, calc_var_with_delta(count(distinct user_id), avg(bcr), var_samp(bcr), avg(n_visit), var_samp(n_visit), covar_samp(bcr, n_visit)) as var_n_bcr
    , calc_var_with_delta(count(distinct user_id), avg(bcr_c), var_samp(bcr_c), avg(n_visit), var_samp(n_visit), covar_samp(bcr_c, n_visit)) as var_n_bcr_c
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_b), var_samp(bcr_b), avg(n_visit), var_samp(n_visit), covar_samp(bcr_b, n_visit)) as var_n_bcr_b
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_via_search), var_samp(bcr_via_search), avg(n_visit), var_samp(n_visit), covar_samp(bcr_via_search, n_visit)) as var_n_bcr_via_search
    , calc_var_with_delta(count(distinct user_id), avg(bcr_via_search_c), var_samp(bcr_via_search_c), avg(n_visit), var_samp(n_visit), covar_samp(bcr_via_search_c, n_visit)) as var_n_bcr_via_search_c
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_via_search_b), var_samp(bcr_via_search_b), avg(n_visit), var_samp(n_visit), covar_samp(bcr_via_search_b, n_visit)) as var_n_bcr_via_search_b
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_via_search_best_match), var_samp(bcr_via_search_best_match), avg(n_visit), var_samp(n_visit), covar_samp(bcr_via_search_best_match, n_visit)) as var_n_bcr_via_search_best_match
    , calc_var_with_delta(count(distinct user_id), avg(bcr_via_search_best_match_c), var_samp(bcr_via_search_best_match_c), avg(n_visit), var_samp(n_visit), covar_samp(bcr_via_search_best_match_c, n_visit)) as var_n_bcr_via_search_best_match_c
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_via_search_best_match_b), var_samp(bcr_via_search_best_match_b), avg(n_visit), var_samp(n_visit), covar_samp(bcr_via_search_best_match_b, n_visit)) as var_n_bcr_via_search_best_match_b
    
    -- buy variance
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy), var_samp(n_buy), avg(n_visit), var_samp(n_visit), covar_samp(n_buy, n_visit)) as var_n_buy
    , calc_var_with_delta(count(distinct user_id), avg(n_buy_c), var_samp(n_buy_c), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_c, n_visit)) as var_n_buy_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_b), var_samp(n_buy_b), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_b, n_visit)) as var_n_buy_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_via_search), var_samp(n_buy_via_search), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_via_search, n_visit)) as var_n_buy_via_search
    , calc_var_with_delta(count(distinct user_id), avg(n_buy_via_search_c), var_samp(n_buy_via_search_c), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_via_search_c, n_visit)) as var_n_buy_via_search_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_via_search_b), var_samp(n_buy_via_search_b), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_via_search_b, n_visit)) as var_n_buy_via_search_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_via_search_best_match), var_samp(n_buy_via_search_best_match), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_via_search_best_match, n_visit)) as var_n_buy_via_search_best_match
    , calc_var_with_delta(count(distinct user_id), avg(n_buy_via_search_best_match_c), var_samp(n_buy_via_search_best_match_c), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_via_search_best_match_c, n_visit)) as var_n_buy_via_search_best_match_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_via_search_best_match_b), var_samp(n_buy_via_search_best_match_b), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_via_search_best_match_b, n_visit)) as var_n_buy_via_search_best_match_b
    
    -- gmv variance
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv), var_samp(n_gmv), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv, n_visit)) as var_n_gmv
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_c), var_samp(n_gmv_c), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_c, n_visit)) as var_n_gmv_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_b), var_samp(n_gmv_b), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_b, n_visit)) as var_n_gmv_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search), var_samp(n_gmv_via_search), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_via_search, n_visit)) as var_n_gmv_via_search
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_c), var_samp(n_gmv_via_search_c), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_via_search_c, n_visit)) as var_n_gmv_via_search_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_b), var_samp(n_gmv_via_search_b), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_via_search_b, n_visit)) as var_n_gmv_via_search_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_best_match), var_samp(n_gmv_via_search_best_match), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_via_search_best_match, n_visit)) as var_n_gmv_via_search_best_match
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_best_match_c), var_samp(n_gmv_via_search_best_match_c), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_via_search_best_match_c, n_visit)) as var_n_gmv_via_search_best_match_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_best_match_b), var_samp(n_gmv_via_search_best_match_b), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_via_search_best_match_b, n_visit)) as var_n_gmv_via_search_best_match_b
    
    -- aov variance
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv), var_samp(n_gmv), avg(n_buy), var_samp(n_buy), covar_samp(n_gmv, n_buy)) as var_n_aov
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_c), var_samp(n_gmv_c), avg(n_buy_c), var_samp(n_buy_c), covar_samp(n_gmv_c, n_buy_c)) as var_n_aov_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_b), var_samp(n_gmv_b), avg(n_buy_b), var_samp(n_buy_b), covar_samp(n_gmv_b, n_buy_b)) as var_n_aov_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search), var_samp(n_gmv_via_search), avg(n_buy_via_search), var_samp(n_buy_via_search), covar_samp(n_gmv_via_search, n_buy_via_search)) as var_n_aov_via_search
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_c), var_samp(n_gmv_via_search_c), avg(n_buy_via_search_c), var_samp(n_buy_via_search_c), covar_samp(n_gmv_via_search_c, n_buy_via_search_c)) as var_n_aov_via_search_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_b), var_samp(n_gmv_via_search_b), avg(n_buy_via_search_b), var_samp(n_buy_via_search_b), covar_samp(n_gmv_via_search_b, n_buy_via_search_b)) as var_n_aov_via_search_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_best_match), var_samp(n_gmv_via_search_best_match), avg(n_buy_via_search_best_match), var_samp(n_buy_via_search_best_match), covar_samp(n_gmv_via_search_best_match, n_buy_via_search_best_match)) as var_n_aov_via_search_best_match
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_best_match_c), var_samp(n_gmv_via_search_best_match_c), avg(n_buy_via_search_best_match_c), var_samp(n_buy_via_search_best_match_c), covar_samp(n_gmv_via_search_best_match_c, n_buy_via_search_best_match_c)) as var_n_aov_via_search_best_match_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_via_search_best_match_b), var_samp(n_gmv_via_search_best_match_b), avg(n_buy_via_search_best_match_b), var_samp(n_buy_via_search_best_match_b), covar_samp(n_gmv_via_search_best_match_b, n_buy_via_search_best_match_b)) as var_n_aov_via_search_best_match_b
    
    -- list variance
    , calc_var_with_delta(count(distinct user_id), avg(lcr), var_samp(lcr), avg(n_visit), var_samp(n_visit), covar_samp(lcr, n_visit)) as var_n_lcr
    , calc_var_with_delta(count(distinct user_id), avg(n_list), var_samp(n_list), avg(n_visit), var_samp(n_visit), covar_samp(n_list, n_visit)) as var_n_list

    -- calculation of variance of handmaid
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_handmaid), var_samp(n_view_handmaid), avg(n_visit), var_samp(n_visit), covar_samp(n_view_handmaid, n_visit)) as var_n_view_handmaid
    , calc_var_with_delta(count(distinct user_id), avg(n_view_handmaid_c), var_samp(n_view_handmaid_c), avg(n_visit), var_samp(n_visit), covar_samp(n_view_handmaid_c, n_visit)) as var_n_view_c_handmaid
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_handmaid_b), var_samp(n_view_handmaid_b), avg(n_visit), var_samp(n_visit), covar_samp(n_view_handmaid_b, n_visit)) as var_n_view_b_handmaid
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_handmaid), var_samp(bcr_handmaid), avg(n_visit), var_samp(n_visit), covar_samp(bcr_handmaid, n_visit)) as var_n_bcr_handmaid
    , calc_var_with_delta(count(distinct user_id), avg(bcr_handmaid_c), var_samp(bcr_handmaid_c), avg(n_visit), var_samp(n_visit), covar_samp(bcr_handmaid_c, n_visit)) as var_n_bcr_handmaid_c
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_handmaid_b), var_samp(bcr_handmaid_b), avg(n_visit), var_samp(n_visit), covar_samp(bcr_handmaid_b, n_visit)) as var_n_bcr_handmaid_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_handmaid), var_samp(n_buy_handmaid), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_handmaid, n_visit)) as var_n_buy_handmaid
    , calc_var_with_delta(count(distinct user_id), avg(n_buy_handmaid_c), var_samp(n_buy_handmaid_c), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_handmaid_c, n_visit)) as var_n_buy_handmaid_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_handmaid_b), var_samp(n_buy_handmaid_b), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_handmaid_b, n_visit)) as var_n_buy_handmaid_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_handmaid), var_samp(n_gmv_handmaid), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_handmaid, n_visit)) as var_n_handmaid_gmv
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_handmaid_c), var_samp(n_gmv_handmaid_c), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_handmaid_c, n_visit)) as var_n_gmv_handmaid_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_handmaid_b), var_samp(n_gmv_handmaid_b), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_handmaid_b, n_visit)) as var_n_gmv_handmaid_b

    -- calculation of variance of food
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_food), var_samp(n_view_food), avg(n_visit), var_samp(n_visit), covar_samp(n_view_food, n_visit)) as var_n_view_food
    , calc_var_with_delta(count(distinct user_id), avg(n_view_food_c), var_samp(n_view_food_c), avg(n_visit), var_samp(n_visit), covar_samp(n_view_food_c, n_visit)) as var_n_view_c_food
    --, calc_var_with_delta(count(distinct user_id), avg(n_view_food_b), var_samp(n_view_food_b), avg(n_visit), var_samp(n_visit), covar_samp(n_view_food_b, n_visit)) as var_n_view_b_food
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_food), var_samp(bcr_food), avg(n_visit), var_samp(n_visit), covar_samp(bcr_food, n_visit)) as var_n_bcr_food
    , calc_var_with_delta(count(distinct user_id), avg(bcr_food_c), var_samp(bcr_food_c), avg(n_visit), var_samp(n_visit), covar_samp(bcr_food_c, n_visit)) as var_n_bcr_food_c
    --, calc_var_with_delta(count(distinct user_id), avg(bcr_food_b), var_samp(bcr_food_b), avg(n_visit), var_samp(n_visit), covar_samp(bcr_food_b, n_visit)) as var_n_bcr_food_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_food), var_samp(n_buy_food), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_food, n_visit)) as var_n_buy_food
    , calc_var_with_delta(count(distinct user_id), avg(n_buy_food_c), var_samp(n_buy_food_c), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_food_c, n_visit)) as var_n_buy_food_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_buy_food_b), var_samp(n_buy_food_b), avg(n_visit), var_samp(n_visit), covar_samp(n_buy_food_b, n_visit)) as var_n_buy_food_b
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_food), var_samp(n_gmv_food), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_food, n_visit)) as var_n_food_gmv
    , calc_var_with_delta(count(distinct user_id), avg(n_gmv_food_c), var_samp(n_gmv_food_c), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_food_c, n_visit)) as var_n_gmv_food_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_gmv_food_b), var_samp(n_gmv_food_b), avg(n_visit), var_samp(n_visit), covar_samp(n_gmv_food_b, n_visit)) as var_n_gmv_food_b

    -- calculation of variance of imp
    --, calc_var_with_delta(count(distinct user_id), avg(n_imp), var_samp(n_imp), avg(n_visit), var_samp(n_visit), covar_samp(n_imp, n_visit)) as var_n_imp
    , calc_var_with_delta(count(distinct user_id), avg(n_imp_c), var_samp(n_imp_c), avg(n_visit), var_samp(n_visit), covar_samp(n_imp_c, n_visit)) as var_n_imp_c
    --, calc_var_with_delta(count(distinct user_id), avg(n_imp_b), var_samp(n_imp_b), avg(n_visit), var_samp(n_visit), covar_samp(n_imp_b, n_visit)) as var_n_imp_b

    -- calculation of number
    --, sum(n_gmv) as sum_gmv
    , sum(n_gmv_c) as sum_gmv_c
    --, sum(n_gmv_b) as sum_gmv_b
    --, sum(n_buy) as sum_buy
    , sum(n_buy_c) as sum_buy_c
    --, sum(n_buy_b) as sum_buy_b
    --, sum(n_view) as sum_view
    , sum(n_view_c) as sum_view_c
    --, sum(n_view_b) as sum_view_b
    --, sum(n_imp) as sum_imp
    , sum(n_imp_c) as sum_imp_c
    --, sum(n_imp_b) as sum_imp_b
  from
    user_act_summary
    left join imp_summary using(user_id, v, type)
  group by 1, 2
)

select *
from summary
  left join seller_summary
      using(type, v)
order by type desc, v
;