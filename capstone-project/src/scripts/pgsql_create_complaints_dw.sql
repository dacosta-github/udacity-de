-- Database: complaints_dw

-- DROP DATABASE complaints_dw;

CREATE DATABASE complaints_dw
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
	
	
CREATE TABLE IF NOT EXISTS dim_companies
(
    company_pk      SERIAL PRIMARY KEY,
    name            VARCHAR NOT NULL,
    public_response VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_geographies
(
    geography_pk SERIAL PRIMARY KEY,
    state        VARCHAR NOT NULL,
    zip_code     VARCHAR NOT NULL,
    county       VARCHAR NOT NULL,
    city         VARCHAR NOT NULL,
    usps         VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_issues
(
    issue_pk  SERIAL PRIMARY KEY,
    issue     VARCHAR NOT NULL,
    sub_issue VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_tags
(
    tag_pk SERIAL PRIMARY KEY,
    tag    VARCHAR NOT NULL

);

CREATE TABLE IF NOT EXISTS dim_products
(
    product_pk  SERIAL PRIMARY KEY,
    product     VARCHAR NOT NULL,
    sub_product VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_complaints
(
    complaint_pk              SERIAL PRIMARY KEY,
    consumer_narrative        VARCHAR NOT NULL,
    consumer_consent_provider VARCHAR NOT NULL,
    submitted_via             VARCHAR NOT NULL

);

CREATE TABLE IF NOT EXISTS dim_dates
(
    date    TIMESTAMP PRIMARY KEY,
    day     INT NOT NULL,
    week    INT NOT NULL,
    month   INT NOT NULL,
    quarter INT NOT NULL,
    year    INT NOT NULL,
    weekday INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_complaints
(

    received_date      TIMESTAMP NOT NULL REFERENCES dim_dates (date),
    sent_date          TIMESTAMP NOT NULL REFERENCES dim_dates (date),
    complaint_pk       INT   NOT NULL REFERENCES dim_complaints (complaint_pk),
    company_pk         INT   NOT NULL REFERENCES dim_companies (company_pk),
    product_pk         INT   NOT NULL REFERENCES dim_products (product_pk),
    issue_pk           INT   NOT NULL REFERENCES dim_issues (issue_pk),
    tag_pk             INT   NOT NULL REFERENCES dim_tags (tag_pk),
    geography_pk       INT   NOT NULL REFERENCES dim_geographies (geography_pk),
    number_issues      INT       NOT NULL,
    is_timely_response INT       NOT NULL

);

CREATE TABLE IF NOT EXISTS stg_raw_complaints
(
    stg_raw_complaints_pk  SERIAL PRIMARY KEY,
    date_received VARCHAR  NULL,
    product	VARCHAR  NULL,
    sub_product	VARCHAR  NULL,
    issue VARCHAR  NULL,
    sub_issue VARCHAR  NULL,
    consumer_complaint_narrative VARCHAR  NULL,
    company_public_response	VARCHAR  NULL,
    company	VARCHAR NULL,
    state VARCHAR NULL,
    zip_code VARCHAR NULL,
    tags VARCHAR NULL,
    consumer_consent_provided VARCHAR  NULL,
    submitted_via VARCHAR  NULL,
    date_sent_to_company VARCHAR  NULL,
    company_response_to_consumer VARCHAR  NULL,	
    timely_response VARCHAR  NULL,
    consumer_disputed VARCHAR  NULL,
    complaint_id VARCHAR  NULL
)


--rank,name,usps,pop2021,pop2010,density,aland
CREATE TABLE IF NOT EXISTS stg_raw_geo_cities
(
    stg_raw_geo_cities_pk  SERIAL PRIMARY KEY,
    rank VARCHAR  NULL,
    name VARCHAR  NULL,
    usps VARCHAR  NULL,
    pop_2021 VARCHAR  NULL,
    pop_2010 VARCHAR  NULL,
    density VARCHAR  NULL,
    aland VARCHAR  NULL
)
--State,pop2021,pop2010,GrowthRate
CREATE TABLE IF NOT EXISTS stg_raw_geo_counties
(
    stg_raw_geo_counties_pk  SERIAL PRIMARY KEY,
    state VARCHAR  NULL,
    pop_2021 VARCHAR  NULL,
    pop_2010 VARCHAR  NULL,
    growth_rate VARCHAR  NULL
)

--State,Abbrev,Code
CREATE TABLE IF NOT EXISTS stg_raw_geo_states
(
    stg_raw_geo_states_pk  SERIAL PRIMARY KEY,
    state VARCHAR  NULL,
    abb_rev VARCHAR  NULL,
    code VARCHAR  NULL
)

--zip,city,county,pop
CREATE TABLE IF NOT EXISTS stg_raw_geo_zip
(
    stg_raw_geo_zip_pk  SERIAL PRIMARY KEY,
    zip VARCHAR  NULL,
    city VARCHAR  NULL,
    county VARCHAR  NULL,
    pop VARCHAR  NULL
)
