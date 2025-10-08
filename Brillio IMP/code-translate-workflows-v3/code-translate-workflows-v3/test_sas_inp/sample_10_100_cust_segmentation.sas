/* Advanced SAS Analysis Pipeline */
%let seg_threshold = 5000;

data customers_raw;
    set raw.customer_data;
    
    /* Data cleaning */
    if missing(customer_id) then delete;
    if missing(region) then region = 'UNKNOWN';
    
    /* Calculate total spend */
    total_spend = sum(spend_cat1, spend_cat2, spend_cat3);
    if total_spend < 0 then total_spend = 0;  /* Fix negative values */
    
    /* Create customer segments */
    if total_spend > &seg_threshold then spend_segment = 'HIGH';
    else if total_spend > (&seg_threshold/2) then spend_segment = 'MEDIUM';
    else spend_segment = 'LOW';
    
    format join_date date9.;
run;

/* Create summary statistics */
proc sql;
    create table segment_summary as
    select 
        region, 
        spend_segment,
        count(customer_id) as segment_size,
        sum(total_spend) as total_segment_revenue,
        mean(total_spend) as avg_customer_spend
    from customers_raw
    group by region, spend_segment;
quit;

/* Highlight high-value segments */
data segment_summary;
    set segment_summary;
    if total_segment_revenue > 100000 then highlight = 'YES';
    else highlight = 'NO';
run;

/* Export results */
%macro format_export(input_table, output_name, format);
    /* Format and prepare data for export */
    data &input_table._final;
        set &input_table;
        report_date = today();
        format report_date date9.;
    run;
    
    /* Export the data */
    proc export data=&input_table._final
        outfile="&output_name..&format"
        dbms=&format replace;
    run;
%mend;

%format_export(segment_summary, segment_analysis, xlsx);