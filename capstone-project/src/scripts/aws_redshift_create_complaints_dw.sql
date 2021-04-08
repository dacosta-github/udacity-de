-- Database: complaints_dw

-- DROP DATABASE complaints_dw;

CREATE DATABASE complaints_dw
    WITH
    OWNER = postgres ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;


drop table if exists fact_complaints;

drop table if exists dim_complaints;
drop table if exists dim_issues;
drop table if exists dim_products;
drop table if exists dim_dates;
drop table if exists dim_geographies;
drop table if exists dim_tags;
drop table if exists dim_companies;

drop table if exists stg_raw_geo_counties;
drop table if exists stg_raw_geo_states;
drop table if exists stg_raw_geo_zip;
drop table if exists stg_raw_complaints;
drop table if exists stg_raw_geo_cities;



CREATE TABLE IF NOT EXISTS dim_companies
(
    company_pk      BIGINT IDENTITY (0,1) NOT NULL,
    name            VARCHAR               NOT NULL,
    public_response VARCHAR               NOT NULL,
    PRIMARY KEY (company_pk)
)
    DISTSTYLE ALL
    SORTKEY (company_pk);

CREATE TABLE IF NOT EXISTS dim_geographies
(
    geography_pk BIGINT IDENTITY (0,1) NOT NULL,
    state        VARCHAR               NOT NULL,
    zip_code     VARCHAR               NOT NULL,
    county       VARCHAR               NOT NULL,
    city         VARCHAR               NOT NULL,
    usps         VARCHAR               NOT NULL,
    PRIMARY KEY (geography_pk)
)
    DISTSTYLE ALL
    SORTKEY (geography_pk);

CREATE TABLE IF NOT EXISTS dim_issues
(
    issue_pk  BIGINT IDENTITY (0,1) NOT NULL,
    issue     VARCHAR               NOT NULL,
    sub_issue VARCHAR,
    PRIMARY KEY (issue_pk)
)
    DISTSTYLE ALL
    SORTKEY (issue_pk);

CREATE TABLE IF NOT EXISTS dim_tags
(
    tag_pk   BIGINT IDENTITY (0,1) NOT NULL,
    tag_desc VARCHAR               NOT NULL,
    PRIMARY KEY (tag_pk)

)
    DISTSTYLE ALL
    SORTKEY (tag_pk);

CREATE TABLE IF NOT EXISTS dim_products
(
    product_pk  BIGINT IDENTITY (0,1) NOT NULL,
    product     VARCHAR               NOT NULL,
    sub_product VARCHAR               NOT NULL,
    PRIMARY KEY (product_pk)
)
    DISTSTYLE ALL
    SORTKEY (product_pk);

CREATE TABLE IF NOT EXISTS dim_complaints
(
    complaint_pk              BIGINT IDENTITY (0,1) NOT NULL,
    consumer_narrative        VARCHAR(max)          NOT NULL,
    consumer_consent_provider VARCHAR               NOT NULL,
    submitted_via             VARCHAR               NOT NULL,
    PRIMARY KEY (complaint_pk)

) DISTSTYLE ALL
  SORTKEY (complaint_pk);

CREATE TABLE IF NOT EXISTS dim_dates
(
    date            DATE,
    year            INT     NOT NULL,
    month           INT     NOT NULL,
    month_name      varchar NOT NULL,
    day_of_month    INT     NOT NULL,
    day_of_week_num INT     NOT NULL,
    day_of_week     varchar NOT NULL,
    week_of_year    INT     not null,
    day_of_year     INT     not null,
    is_weekend      bool    not null,
    quarter         INT     NULL,
    PRIMARY KEY (date)
) DISTSTYLE ALL
  SORTKEY (date);

CREATE TABLE IF NOT EXISTS fact_complaints
(

    received_date      TIMESTAMP NOT NULL REFERENCES dim_dates (date),
    sent_date          TIMESTAMP NOT NULL REFERENCES dim_dates (date),
    complaint_pk       INT       NOT NULL REFERENCES dim_complaints (complaint_pk),
    company_pk         INT       NOT NULL REFERENCES dim_companies (company_pk),
    product_pk         INT       NOT NULL REFERENCES dim_products (product_pk),
    issue_pk           INT       NOT NULL REFERENCES dim_issues (issue_pk),
    tag_pk             INT       NOT NULL REFERENCES dim_tags (tag_pk),
    geography_pk       INT       NOT NULL REFERENCES dim_geographies (geography_pk),
    number_issues      INT       NOT NULL,
    is_timely_response INT       NOT NULL
) DISTKEY (complaint_pk)
  SORTKEY (complaint_pk, received_date);

--drop table stg_raw_complaints
CREATE TABLE IF NOT EXISTS stg_raw_complaints
(
    stg_raw_complaints_pk        BIGINT IDENTITY (0,1) NOT NULL,
    date_received                VARCHAR(max)          NULL,
    product                      VARCHAR(max)          NULL,
    sub_product                  VARCHAR(max)          NULL,
    issue                        VARCHAR(max)          NULL,
    sub_issue                    VARCHAR(max)          NULL,
    consumer_complaint_narrative VARCHAR(max)          NULL,
    company_public_response      VARCHAR(max)          NULL,
    company                      VARCHAR(max)          NULL,
    state                        VARCHAR(max)          NULL,
    zip_code                     VARCHAR(max)          NULL,
    tags                         VARCHAR(max)          NULL,
    consumer_consent_provided    VARCHAR(max)          NULL,
    submitted_via                VARCHAR(max)          NULL,
    date_sent_to_company         VARCHAR(max)          NULL,
    company_response_to_consumer VARCHAR(max)          NULL,
    timely_response              VARCHAR(max)          NULL,
    consumer_disputed            VARCHAR(max)          NULL,
    complaint_id                 VARCHAR(max)          NULL,
    PRIMARY KEY (stg_raw_complaints_pk)
) DISTSTYLE EVEN;


--rank,name,usps,pop2021,pop2010,density,aland
CREATE TABLE IF NOT EXISTS stg_raw_geo_cities
(
    stg_raw_geo_cities_pk BIGINT IDENTITY (0,1) NOT NULL,
    rank                  VARCHAR               NULL,
    name                  VARCHAR               NULL,
    usps                  VARCHAR               NULL,
    pop_2021              VARCHAR               NULL,
    pop_2010              VARCHAR               NULL,
    density               VARCHAR               NULL,
    aland                 VARCHAR               NULL,
    PRIMARY KEY (stg_raw_geo_cities_pk)
) DISTSTYLE EVEN;

--State,pop2021,pop2010,GrowthRate
CREATE TABLE IF NOT EXISTS stg_raw_geo_counties
(
    stg_raw_geo_counties_pk BIGINT IDENTITY (0,1) NOT NULL,
    state                   VARCHAR               NULL,
    pop_2021                VARCHAR               NULL,
    pop_2010                VARCHAR               NULL,
    growth_rate             VARCHAR               NULL,
    PRIMARY KEY (stg_raw_geo_counties_pk)
) DISTSTYLE EVEN;

--State,Abbrev,Code
CREATE TABLE IF NOT EXISTS stg_raw_geo_states
(
    stg_raw_geo_states_pk BIGINT IDENTITY (0,1) NOT NULL,
    state                 VARCHAR               NULL,
    abb_rev               VARCHAR               NULL,
    code                  VARCHAR               NULL,
    PRIMARY KEY (stg_raw_geo_states_pk)
) DISTSTYLE EVEN;

--zip,city,county,pop
CREATE TABLE IF NOT EXISTS stg_raw_geo_zip
(
    stg_raw_geo_zip_pk BIGINT IDENTITY (0,1) NOT NULL,
    zip                VARCHAR               NULL,
    city               VARCHAR               NULL,
    county             VARCHAR               NULL,
    pop                VARCHAR               NULL,
    PRIMARY KEY (stg_raw_geo_zip_pk)
) DISTSTYLE EVEN;


CREATE TABLE IF NOT EXISTS fact_complaints
(

    received_date      TIMESTAMP NOT NULL REFERENCES dim_dates (date),
    sent_date          TIMESTAMP NOT NULL REFERENCES dim_dates (date),
    complaint_pk       INT       NOT NULL REFERENCES dim_complaints (complaint_pk),
    company_pk         INT       NOT NULL REFERENCES dim_companies (company_pk),
    product_pk         INT       NOT NULL REFERENCES dim_products (product_pk),
    issue_pk           INT       NOT NULL REFERENCES dim_issues (issue_pk),
    tag_pk             INT       NOT NULL REFERENCES dim_tags (tag_pk),
    geography_pk       INT       NOT NULL REFERENCES dim_geographies (geography_pk),
    number_issues      INT       NOT NULL,
    is_timely_response INT       NOT NULL
) DISTKEY (complaint_pk)
  SORTKEY (complaint_pk, received_date);




----> View
CREATE OR REPLACE VIEW public.v_fact_tableau
as
select f.name
     , f.public_response
     , received_date
     , sent_date
     , number_issues
     , is_timely_response
     , consumer_narrative
     , consumer_consent_provider
     , submitted_via
     , state
     , zip_code
     , county
     , city
     , usps
     , product
     , sub_product
     , issue
     , sub_issue
     , tag_desc
from fact_complaints as a
         join dim_companies as f on f.company_pk = a.company_pk
         join dim_complaints as b on b.complaint_pk = a.complaint_pk
         join dim_geographies as g on g.geography_pk = a.geography_pk
         join dim_products as p on p.product_pk = a.product_pk
         join dim_issues as i on i.issue_pk = a.issue_pk
         join dim_tags as t on t.tag_pk = a.tag_pk
