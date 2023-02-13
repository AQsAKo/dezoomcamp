select count(distinct Affiliated_base_number) from dezoom-3.fhv_taxi.fares_non_part;

select count(1) from dezoom-3.fhv_taxi.fares_non_part where PUlocationID is NULL and DOlocationID is NULL;

create or replace table dezoom-3.fhv_taxi.fares_p_c 
partition by date(pickup_datetime)
cluster by Affiliated_base_number
as
select * from dezoom-3.fhv_taxi.fares_non_part;

select count(distinct Affiliated_base_number) from dezoom-3.fhv_taxi.fares_non_part
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';

select count(distinct Affiliated_base_number) from dezoom-3.fhv_taxi.fares_p_c
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';
