class SqlLoadFacts:
"""
    This module contains all sql queries/statements
"""
    insert_fact_complaints_table = ("""
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

    """)