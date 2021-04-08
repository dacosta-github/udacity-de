class SqlDdl:
"""
    This module contains all sql queries/statements
"""

# DROP TABLES
# Staging
stg_raw_geo_counties_table_drop = "DROP TABLE IF EXISTS public.stg_raw_geo_counties;"
stg_raw_geo_states_table_drop = "DROP TABLE IF EXISTS public.stg_raw_geo_states;"
stg_raw_geo_zip_table_drop = "DROP TABLE IF EXISTS public.stg_raw_geo_zip;"
stg_raw_geo_cities_table_drop = "DROP TABLE IF EXISTS public.stg_raw_geo_cities;"
stg_raw_complaints_table_drop = "DROP TABLE IF EXISTS public.stg_raw_complaints;"

# Factual
fact_complaints_table_drop = "DROP TABLE IF EXISTS public.fact_complaints;"

# Dimensions
dim_complaints_table_drop = "DROP TABLE IF EXISTS public.dim_complaints;"
dim_issues_table_drop = "DROP TABLE IF EXISTS public.dim_issues;"
dim_geographies_table_drop = "DROP TABLE IF EXISTS public.dim_geographies;"
dim_tags_table_drop = "DROP TABLE IF EXISTS public.dim_tags;"
dim_companies_table_drop = "DROP TABLE IF EXISTS public.dim_companies;"
dim_products_table_drop = "DROP TABLE IF EXISTS public.dim_products;"
dim_dates_table_drop = "DROP TABLE IF EXISTS public.dim_dates;"


# CREATE 
# STAGING TABLES
create_stg_raw_geo_counties_table = ("""
   CREATE TABLE IF NOT EXISTS stg_raw_geo_counties
    (
        stg_raw_geo_counties_pk BIGINT IDENTITY (0,1) NOT NULL,
        state                   VARCHAR               NULL,
        pop_2021                VARCHAR               NULL,
        pop_2010                VARCHAR               NULL,
        growth_rate             VARCHAR               NULL,
        PRIMARY KEY (stg_raw_geo_counties_pk)
    ) DISTSTYLE EVEN;
""")

create_stg_raw_geo_cities_table = (""" 
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
""")

create_stg_raw_geo_states_table = (""" 
    CREATE TABLE IF NOT EXISTS stg_raw_geo_states
    (
        stg_raw_geo_states_pk BIGINT IDENTITY (0,1) NOT NULL,
        state                 VARCHAR               NULL,
        abb_rev               VARCHAR               NULL,
        code                  VARCHAR               NULL,
        PRIMARY KEY (stg_raw_geo_states_pk)
    ) DISTSTYLE EVEN;
""")

create_stg_raw_geo_zip_table = (""" 
    CREATE TABLE IF NOT EXISTS stg_raw_geo_zip
    (
        stg_raw_geo_zip_pk BIGINT IDENTITY (0,1) NOT NULL,
        zip                VARCHAR               NULL,
        city               VARCHAR               NULL,
        county             VARCHAR               NULL,
        pop                VARCHAR               NULL,
        PRIMARY KEY (stg_raw_geo_zip_pk)
    ) DISTSTYLE EVEN;
""")

create_stg_raw_complaints_table = (""" 
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
""")



## DIMENSIONS
create_dim_complaints_table = ("""
   CREATE TABLE IF NOT EXISTS dim_complaints
    (
        complaint_pk              BIGINT IDENTITY (0,1) NOT NULL,
        consumer_narrative        VARCHAR(max)          NOT NULL,
        consumer_consent_provider VARCHAR               NOT NULL,
        submitted_via             VARCHAR               NOT NULL,
        PRIMARY KEY (complaint_pk)

    ) DISTSTYLE ALL
    SORTKEY (complaint_pk);  
""")

create_dim_issues_table = ("""
     CREATE TABLE IF NOT EXISTS dim_issues
    (
        issue_pk  BIGINT IDENTITY (0,1) NOT NULL,
        issue     VARCHAR               NOT NULL,
        sub_issue VARCHAR,
        PRIMARY KEY (issue_pk)
    )
        DISTSTYLE ALL
        SORTKEY (issue_pk);
""")

create_dim_products_table = ("""
     CREATE TABLE IF NOT EXISTS dim_products
    (
        product_pk  BIGINT IDENTITY (0,1) NOT NULL,
        product     VARCHAR               NOT NULL,
        sub_product VARCHAR               NOT NULL,
        PRIMARY KEY (product_pk)
    )
        DISTSTYLE ALL
        SORTKEY (product_pk);
""")

create_dim_dates_table = ("""
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
""")

create_dim_geographies_table = ("""
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
""")

create_dim_tags_table = ("""
     CREATE TABLE IF NOT EXISTS dim_tags
    (
        tag_pk   BIGINT IDENTITY (0,1) NOT NULL,
        tag_desc VARCHAR               NOT NULL,
        PRIMARY KEY (tag_pk)

    )
        DISTSTYLE ALL
        SORTKEY (tag_pk);
""")

create_dim_companies_table = ("""
    CREATE TABLE IF NOT EXISTS dim_companies
    (
        company_pk      BIGINT IDENTITY (0,1) NOT NULL,
        name            VARCHAR               NOT NULL,
        public_response VARCHAR               NOT NULL,
        PRIMARY KEY (company_pk)
    )
        DISTSTYLE ALL
        SORTKEY (company_pk);
     
""")

## Factual
create_fact_complaints_table  = ("""
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
        
""")
