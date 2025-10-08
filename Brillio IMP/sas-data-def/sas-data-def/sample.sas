/* ============================================================= */
/* Extended SAS script for Customer Segmentation & Enrichment    */
/* ============================================================= */

/* Global macro variable for threshold */
%let seg_threshold = 1000;

/* Step 1: Import and clean customer data */
data customers_clean;
    set raw.customer_data;
    where status = 'ACTIVE';
    total_spend = sum(spend_cat1, spend_cat2, spend_cat3);
    if total_spend < 0 then total_spend = 0;
    if missing(region) then region = 'UNKNOWN';
    format join_date date9.;
run;

/* Step 1a: Import transaction data */
data transactions_clean;
    set raw.transaction_data;
    where txn_status = 'SUCCESS';
    txn_month = month(txn_date);
    txn_year  = year(txn_date);
    if amount < 0 then amount = 0;
run;

/* Step 1b: Import loyalty program data */
data loyalty_clean;
    set raw.loyalty_data;
    where enrolled = 1;
    if missing(loyalty_points) then loyalty_points = 0;
run;

/* Step 2: Merge customer and transaction data */
proc sql;
    create table customer_txn_summary as
    select 
        a.customer_id,
        a.region,
        a.total_spend,
        count(distinct b.txn_id) as txn_count,
        sum(b.amount) as txn_total,
        avg(b.amount) as avg_txn_amount
    from customers_clean a
    left join transactions_clean b
        on a.customer_id = b.customer_id
    group by a.customer_id, a.region, a.total_spend;
quit;

/* Step 2a: Merge with loyalty data */
proc sql;
    create table customer_full_profile as
    select 
        c.*,
        l.loyalty_points,
        case 
            when l.loyalty_points >= 5000 then 'GOLD'
            when l.loyalty_points >= 1000 then 'SILVER'
            else 'BRONZE'
        end as loyalty_tier
    from customer_txn_summary c
    left join loyalty_clean l
        on c.customer_id = l.customer_id;
quit;

/* Step 3: Create customer segments */
proc sql;
    create table customer_segments as
    select 
        customer_id,
        region,
        total_spend,
        txn_total,
        loyalty_tier,
        case 
            when total_spend > &seg_threshold then 'HIGH'
            when total_spend > (&seg_threshold/2) then 'MEDIUM'
            else 'LOW'
        end as spend_segment,
        count(*) as segment_size
    from customer_full_profile
    group by calculated spend_segment, region, loyalty_tier
    order by region, loyalty_tier, calculated spend_segment;
quit;

/* Step 4: Create summary report */
proc summary data=customer_segments nway;
    class region spend_segment loyalty_tier;
    var total_spend txn_total;
    output out=segment_summary
           sum(total_spend txn_total)=total_segment_revenue total_txn_revenue
           mean(total_spend txn_total)=avg_customer_spend avg_txn_value;
run;

/* Step 5: Define report formatting macro */
%macro format_report(input_ds, output_ds, title_text);
    data &output_ds;
        set &input_ds;
        report_title = "&title_text";
        report_date = today();
        format report_date date9.;
        if total_segment_revenue > 100000 then highlight = 'YES';
        else highlight = 'NO';
    run;

    proc print data=&output_ds;
        title "&title_text";
        var region spend_segment loyalty_tier 
            total_segment_revenue total_txn_revenue 
            avg_customer_spend avg_txn_value highlight;
    run;
%mend;

/* Step 6: Generate formatted report */
%format_report(segment_summary, final_report, Customer Segment Analysis with Loyalty & Transactions);

/* Step 7: Export results */
proc export data=final_report
    outfile="segment_analysis_extended.xlsx"
    dbms=xlsx
    replace;
run;
