-------- Change these dates and experiment name for each experiment 
-- target_from() should be the next day of test start date
create temporary function target_os_for_laplace() as (['DEVICE_IOS']); -- DEVICE_ANDROID,DEVICE_IOS
create temporary function target_os_for_api() as (['PLATFORM_IOS']);
create temporary function target_from() AS (date('2022-12-08'));
create temporary function target_to() AS (date('2022-12-16'));
create temporary function experiment_name() AS ('EDGE-2434_mercari_IME');

--使いそうなもの
create temporary function is_item_like_tap_event(l string, t string) as (l = 'item_details:item_info' and t = 'ITEM_LIKE');
create temporary function is_item_detail_display_event(l string, t string) as (l = 'item_details' and t = 'ITEM_VIEW');
--listing
create temporary function is_liststart_event(lr string, t string) as (lr = 'LISTING_MODE_NEW' and t = 'LISTING_START');
create temporary function is_listcomp_event(lr string, t string) as (lr = 'LISTING_RESULT_NEW_LISTING' and t = 'LISTING_END');
--editing
create temporary function is_edit_event(lr string, t string) as (lr = 'TAP' and t = 'item_details:item_info:product_edit');
create temporary function is_editcomp_event(lr string, t string) as (lr = 'LISTING_RESULT_UPDATE' and t = 'LISTING_END');
--buying
create temporary function is_buy_complete_event(l string, t string) as (l in ('', 'purchase') and t = 'PURCHASE_COMPLETED');

with 
users_and_variants as (

select
    distinct context.uuid as uuid,
    context.user_id as user_id,
    event.experiment_variant as variant,
    min(datetime(server_time,'Asia/Tokyo')) as assign_time
from
    `mercari-data-infra-prod.events.ablog_v2`
where
    event.type = "EXPERIMENT_CHECK"
    and event.experiment_id = experiment_name()
    and date(server_time, 'Asia/Tokyo') between target_from() and target_to()
    and event.experiment_variant in (1,2) 
    and context.type in unnest(target_os_for_laplace())
group by 
  uuid,
  user_id,
  variant
having
  min(event.experiment_variant) = max(event.experiment_variant)
),

laplace as(
select  
  distinct
    context.uuid as uuid,
    context.user_id as user_id,
    event.item_id as item_id,
    event.listing_session_id as listing_session_id,
    datetime(server_time,'Asia/Tokyo') as daytime,
    variant,
    case
        when is_item_detail_display_event(event.location,event.type) then 'item_detail_view'
        when is_edit_event(event.type,event.location) then 'edit_start'
        when is_editcomp_event(event.listing_result, event.type) then 'edit_comp'
        when is_liststart_event(event.listing_mode,event.type) then 'list_start'
        when is_listcomp_event(event.listing_result, event.type) then 'list_comp'
    end as flg
from 
  `mercari-data-infra-prod.events.client_events_v2` as lp
    left join (select uuid,variant, min(assign_time) as min_assign_time from users_and_variants group by uuid,variant) as uv on lp.context.uuid = uv.uuid
where 
  date(server_time, 'Asia/Tokyo') between target_from() and target_to()
  and min_assign_time <= datetime(server_time,'Asia/Tokyo')
  and context.uuid <> ""
  and --以下で必要なデータにする
  (is_item_detail_display_event(event.location,event.type) -- item detail view
  or (is_edit_event(event.type,event.location)) -- edit start
  or (is_editcomp_event(event.listing_result, event.type)) -- edit complete
  or (is_liststart_event(event.listing_mode,event.type)) -- list start
  or (is_listcomp_event(event.listing_result, event.type)) -- list end
))

--uuidを入れるために使う
, list_start as 
(
select
  uuid,
  count(distinct listing_session_id) as n_list_start_sessions_during_experiment
from
 laplace
where
  flg = 'list_start'
group by 
  uuid 
)

, list_comp as 
(
select
  distinct 
    uuid,
    item_id,
    daytime,
    n_listings_during_experiment
from
  laplace
    left join (select uuid,count(distinct item_id) as n_listings_during_experiment from laplace where flg = 'list_comp' group by uuid) using(uuid)
where
  flg = 'list_comp'
)

, sellings as
(
select
  cast(seller_id as string) as user_id,
  count(distinct case when date(created,'Asia/Tokyo') between target_from() and target_to()then item_id end) as n_solds_during_experiment
from
  `kouzoh-analytics-jp-prod.components_listing_and_buying.transaction_evidences_with_canceled` as transactions
    left join (select user_id,min(assign_time) as min_assign_time from users_and_variants group by user_id) as users_and_variants
      on cast(transactions.seller_id as string) = users_and_variants.user_id
where 
  min_assign_time <= datetime(created)
group by
  seller_id)

,tx as(
select 
  distinct item_id 
  , date(created,'Asia/Tokyo') as created 
  , cast(seller_id as string) as seller_id
from 
  `kouzoh-analytics-jp-prod.components_listing_and_buying.transaction_evidences_with_canceled`  )

--自分の売れる前のitem_detail見た人
,own_item_visitors as 
  (
  select   
    laplace.uuid,
    count(distinct laplace.item_id) as n_view_item_cnt_during_experiment,
    count(distinct laplace.daytime) as n_view_item_session_during_experiment
  from 
    (select uuid,item_id,daytime from laplace where flg = 'item_detail_view') as laplace
      inner join list_comp using(item_id)
      left join tx using(item_id)
  where 
    laplace.uuid = list_comp.uuid
    and (laplace.daytime < tx.created or tx.item_id is null)
  group by 
    uuid
)

--editcompした人
, edit_comp as 
  (
  select 
    laplace.uuid,
    count(distinct laplace.item_id) as n_editcomp_item_cnt_during_experiment,
    count(distinct laplace.daytime) as n_editcomp_session_during_experiment
  from 
    (select uuid,item_id,daytime from laplace where flg = 'edit_comp') as laplace
      inner join list_comp using(item_id)
      left join tx using(item_id)
  where 
    laplace.uuid = list_comp.uuid
  group by 
    uuid
  )

--edit start
, edit_start as(
select 
  laplace.uuid,
  count(distinct laplace.item_id) as n_editstart_item_cnt_during_experiment,
  count(distinct laplace.daytime) as n_editstart_session_during_experiment
from 
   (select uuid,item_id,daytime from laplace where flg = 'edit_start') as laplace
    inner join list_comp using(item_id)
where 
  laplace.uuid = list_comp.uuid
group by 
  uuid
)
select
  distinct
    --uv.user_id,
    uv.uuid,
    variant, 
    --first_listed_day,
    --if(first_listed_day is null or first_listed_day>= target_from() ,true, false) as was_never_lister,

    --list
    coalesce(n_list_start_sessions_during_experiment,0) as n_list_start_sessions_during_experiment,
    coalesce(lc.n_listings_during_experiment,0) as n_listings_during_experiment,
    --other
    coalesce(n_solds_during_experiment,0) as n_solds_during_experiment,
    coalesce(n_view_item_cnt_during_experiment,0) as n_view_item_cnt_during_experiment,
    --edit
    coalesce(n_editstart_item_cnt_during_experiment,0) as n_editstart_item_cnt_during_experiment,
    coalesce(n_editcomp_item_cnt_during_experiment,0) as n_editcomp_item_cnt_during_experiment,
    experiment_name()

from
  users_and_variants uv
    left join sellings as s on uv.user_id =  s.user_id --変更
    left join list_start as ls on uv.uuid = ls.uuid 
    left join list_comp as lc on uv.uuid = lc.uuid 
    left join own_item_visitors as iv on uv.uuid = iv.uuid 
    left join edit_start as es on uv.uuid = es.uuid 
    left join edit_comp as ec on uv.uuid = ec.uuid 