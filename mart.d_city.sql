insert into mart.d_city (city_id, city_name)
select city_id, city_name from staging.user_order_log
where city_id not in (select city_id from mart.d_city)
group by city_id, city_name;
WHERE DATE(date_time) = '{{ date }}'::date
  AND city_id NOT IN (SELECT city_id FROM mart.d_city)