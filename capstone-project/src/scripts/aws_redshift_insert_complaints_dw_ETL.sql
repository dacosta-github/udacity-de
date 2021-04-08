------------------
---- ETL ---------
------------------

-- STG:
truncate table stg_raw_complaints;
COPY stg_raw_complaints (date_received, product, sub_product, issue, sub_issue, consumer_complaint_narrative,
                         company_public_response, company, state, zip_code, tags, consumer_consent_provided,
                         submitted_via, date_sent_to_company, company_response_to_consumer, timely_response,
                         consumer_disputed, complaint_id)
    FROM 's3://complaints-raw-datalake/complaints/complaints_part_0.csv'
    WITH credentials 'aws_access_key_id=xxxxxxx;aws_secret_access_key=yyyyyyy'
    FORMAT as CSV
    DELIMITER as '|' QUOTE as '\"' IGNOREHEADER 1
    REGION as 'eu-west-1';


truncate table stg_raw_geo_cities;
COPY stg_raw_geo_cities (rank, name, usps, pop_2021, pop_2010, density, aland)
    FROM 's3://complaints-raw-datalake/cities/geo_cities_part_01.csv'
    WITH credentials 'aws_access_key_id=xxxxxxx;aws_secret_access_key=yyyyyyy'
    FORMAT as CSV
    DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1
    REGION as 'eu-west-1';


truncate table stg_raw_geo_counties;
COPY stg_raw_geo_counties (state, pop_2021, pop_2010, growth_rate)
    FROM 's3://complaints-raw-datalake/counties/geo_counties_part_01.csv'
    WITH credentials 'aws_access_key_id=xxxxxxx;aws_secret_access_key=yyyyyyy'
    FORMAT as CSV
    DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1
    REGION as 'eu-west-1';

truncate table stg_raw_geo_zip;
COPY stg_raw_geo_zip (zip, city, county, pop)
    FROM 's3://complaints-raw-datalake/zip/geo_zip_part_01.csv'
    WITH credentials 'aws_access_key_id=xxxxxxx;aws_secret_access_key=yyyyyyy'
    FORMAT as CSV
    DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1
    REGION as 'eu-west-1';
    

truncate table stg_raw_geo_states;
COPY stg_raw_geo_states (state, abb_rev, code)
    FROM 's3://complaints-raw-datalake/states/geo_states_part_01.csv'
    WITH credentials 'aws_access_key_id=xxxxxxx;aws_secret_access_key=yyyyyyy'
    FORMAT as CSV
    DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1
    REGION as 'eu-west-1';

/*
    select *
    from stl_load_errors
*/


----> dimensions::

--dim_geographies
insert into dim_geographies(state, zip_code, county, city, usps)
select *
from (
         select st.state  as state,
                gz.zip    as zip_code,
                gz.county as county,
                ct.name   as city,
                ct.usps   as code
         from stg_raw_geo_zip as gz
                  inner join stg_raw_geo_cities as ct
                             on ct.name = gz.city
                  inner join stg_raw_geo_states st
                             on st.code = ct.usps
                  inner join stg_raw_geo_counties srgc
                             on srgc.state = st.state
         group by st.state,
                  gz.zip,
                  gz.county,
                  ct.name,
                  ct.usps
         union all
         select 'Unknown' as state,
                'Unknown' as zip_code,
                'Unknown' as county,
                'Unknown' as city,
                'Unknown' as code
     ) as total
Where 1 = 1
  and state + '-' + zip_code + '-' + county + '-' + city + '-' + code not in
      (select state + '-' + zip_code + '-' + county + '-' + city + '-' + code from dim_geographies);

-- dim_companies
insert into dim_companies(name, public_response)
select *
from (select btrim(c.company)                              as name,
             case
                 when c.company_public_response = '' then 'N/A'
                 else btrim(c.company_public_response) end as public_response
      from stg_raw_complaints as c
      group by c.company, c.company_public_response
      union all
      select 'Unknown' as name,
             'Unknown' as public_response
     ) as total
where 1 = 1
  and name + '-' + public_response not in (select name + '-' + public_response from dim_companies);

-- dim_products
insert into dim_products(product, sub_product)
select *
from (
         select btrim(c.product)                                                      as product,
                case when c.sub_product = '' then 'N/A' else btrim(c.sub_product) end as sub_product
         from public.stg_raw_complaints as c
         group by c.product, c.sub_product
         union all
         select 'Unknown' as product,
                'Unknown' as sub_product
     ) as Total
where 1 = 1
  and product + '-' + sub_product not in (select product + '-' + sub_product from dim_products);

-- dim_issues
insert into dim_issues(issue, sub_issue)
select *
from (
         select btrim(c.issue)                                                    as issue,
                case when c.sub_issue = '' then 'N/A' else btrim(c.sub_issue) end as sub_issue
         from public.stg_raw_complaints as c
         group by c.issue, c.sub_issue
         union all
         select 'Unknown' as issue,
                'Unknown' as sub_issue
     ) as Total
where 1 = 1
  and issue + '-' + sub_issue not in (select issue + '-' + sub_issue from dim_issues);


-- dim_issues
insert into dim_tags(tag_desc)
select *
from (
         select case when c.tags = '' then 'N/A' else btrim(c.tags) end as tag_desc
         from public.stg_raw_complaints as c
         group by c.tags
         union all
         select 'Unknown' as tag_desc
     ) as Total
where 1 = 1
  and tag_desc not in (select tag_desc from dim_tags);

-- dim_complaints
insert into dim_complaints(consumer_narrative, consumer_consent_provider, submitted_via)
select *
from (
         select case
                    when c.consumer_complaint_narrative = '' then 'Hidden'
                    else btrim(c.consumer_complaint_narrative) end as consumer_narrative,
                case
                    when c.consumer_consent_provided = '' then 'N/A'
                    else btrim(c.consumer_consent_provided) end    as consumer_consent_provider,
                case
                    when c.submitted_via = '' then 'N/A'
                    else btrim(c.submitted_via) end                as submitted_via
         from stg_raw_complaints as c
         group by c.consumer_complaint_narrative, c.consumer_consent_provided, c.submitted_via
         union all
         select 'Unknown' as consumer_narrative,
                'Unknown' as consumer_consent_provider,
                'Unknown' as submitted_via
     ) as Total
where 1 = 1
  and consumer_narrative + '-' + consumer_consent_provider + '-' + submitted_via not in
      (select consumer_narrative + '-' + consumer_consent_provider + '-' + submitted_via from dim_complaints);


-- dim_complaints
insert into dim_dates(date, year, month, month_name, day_of_month, day_of_week_num, day_of_week, week_of_year,
                      day_of_year, is_weekend, quarter)
select *
from (
         select dat                                                  as date,
                date_part(year, dat)                                 as year,
                date_part(mm, dat)                                   as month,
                to_char(dat, 'Mon')                                  as month_name,
                date_part(day, dat)                                  as day_of_month,
                date_part(dow, dat)                                  as day_of_week_num,
                to_char(dat, 'Day')                                  as day_of_week,
                date_part(week, dat)                                 as week_of_year,
                date_part(doy, dat)                                  as day_of_year,
                decode(date_part(dow, dat), 0, true, 6, true, false) as is_weekend,
                date_part(QUARTER, dat)                              as quater
         from (select trunc(dateadd(day, ROW_NUMBER() over () - 1, '2010-01-01')) as dat
               from stg_raw_complaints
              )
         UNION ALL
         select '1900-01-01' as date,
                1900         as year,
                01           as month,
                'Jan'        as month_name,
                01           as day_of_month,
                01           as day_of_week_num,
                'Sunday'     as day_of_week,
                01           as week_of_year,
                01           as day_of_year,
                True         as is_weekend,
                01           as quater
     ) as total
where date not in (select distinct date from dim_dates);


insert into fact_complaints(received_date, sent_date, complaint_pk, company_pk, product_pk, issue_pk, tag_pk,
                            geography_pk, number_issues, is_timely_response)
select c.date                                                   as received_date,
       d.date                                                   as sent_date,
       b.complaint_pk,
       e.company_pk,
       f.product_pk,
       g.issue_pk,
       i.tag_pk,
       coalesce(h.geography_pk, 231)                            as geography_pk,
       count(distinct a.complaint_id)                             as number_issues,
       sum(case when lower(a.timely_response) = 'yes' then 1 else 0 end) as is_timely_response
from stg_raw_complaints as a
         join dim_complaints as b
              on case
                     when a.consumer_complaint_narrative = '' then 'Hidden'
                     else btrim(a.consumer_complaint_narrative) end = b.consumer_narrative
                  and
                 case
                     when a.consumer_consent_provided = '' then 'N/A'
                     else btrim(a.consumer_consent_provided) end = b.consumer_consent_provider
                  and case
                          when a.submitted_via = '' then 'N/A'
                          else btrim(a.submitted_via) end = b.submitted_via
         join dim_dates as c
              on c.date = a.date_received
         join dim_dates as d
              on d.date = a.date_sent_to_company
         join dim_companies as e
              on btrim(e.name) = btrim(a.company)
         join dim_products as f
              on case when btrim(a.product) = '' then 'N/A' else btrim(a.product) end = f.product
                  and case when btrim(a.sub_product) = '' then 'N/A' else btrim(a.sub_product) end = f.sub_product
         join dim_issues as g
              on case when btrim(a.issue) = '' then 'N/A' else btrim(a.issue) end = g.issue
                  and case when btrim(a.sub_issue) = '' then 'N/A' else btrim(a.sub_issue) end = g.sub_issue
         left join dim_geographies as h
                   on h.zip_code = a.zip_code
         left join dim_tags as i
                   on i.tag_desc = case when A.tags = '' then 'N/A' else btrim(A.tags) end
GROUP BY b.complaint_pk,
         c.date,
         d.date,
         e.company_pk,
         f.product_pk,
         g.issue_pk,
         h.geography_pk,
         i.tag_pk;
