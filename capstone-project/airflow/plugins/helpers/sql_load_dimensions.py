class SqlLoadDimensions:
"""
    This module contains all sql queries/statements
"""
    insert_dim_geographies_table = ("""
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
    """)

    insert_dim_companies_table = ("""
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
                )
            where 1 = 1
            and name + '-' + public_response not in (select name + '-' + public_response from dim_companies);
    """)

    insert_dim_issues_table = ("""
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
    """)

    insert_dim_products_table = ("""
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
    """)


    insert_dim_dates_table = ("""
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
    """)

    insert_dim_tags_table = ("""
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
    """)

    insert_dim_complaints_table = ("""
        select *
        from (
                select case
                            when c.consumer_complaint_narrative = '' then 'Hidden'
                            else btrim(c.consumer_complaint_narrative) end                                                as consumer_narrative,
                        case
                            when c.consumer_consent_provided = '' then 'N/A'
                            else btrim(c.consumer_consent_provided) end                                                   as consumer_consent_provider,
                        case
                            when c.submitted_via = '' then 'N/A'
                            else btrim(c.submitted_via) end                                                               as submitted_via
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
    """)