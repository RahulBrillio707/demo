/* Test case 1: Simple nested macro */
%macro outer1;
    data test1;
        set work.data1;
        if x > 0 then do;
            y = x * 2;
        end;
    run;
%mend outer1;

/* Test case 2: Nested macros */
%macro outer2;
    %let var1 = value1;
    %macro inner2;
        data test2;
            set work.data2;
        run;
    %mend inner2;
    %inner2;
%mend outer2;

/* Test case 3: Macro with PROC SQL and nested structures */
%macro complex;
    %if &condition = YES %then %do;
        proc sql;
            create table work.test as
            select *
            from work.source
            where date > today();
        quit;
        
        data final;
            set work.test;
            if status = 'A' then do;
                flag = 1;
            end;
            else do;
                flag = 0;
            end;
        run;
    %end;
%mend complex;

/* Test case 4: Triple nested with mixed constructs */
%macro level1;
    %let level = 1;
    %macro level2;
        %let level = 2;
        %if &debug = YES %then %do;
            data debug_info;
                level = &level;
                proc_name = "test";
                do i = 1 to 10;
                    value = i * level;
                    if value > 5 then do;
                        category = 'HIGH';
                    end;
                    else do;
                        category = 'LOW';
                    end;
                    output;
                end;
            run;
        %end;
    %mend level2;
    %level2;
%mend level1;

/* Test case 5: Problematic nesting (potential mismatches) */
%macro problematic;
    data step1;
        set work.input;
        /* Missing RUN - should this end the data step? */
    
    %if &mode = BATCH %then %do;
        proc print data=step1;
        /* Missing RUN/QUIT */
    %end;
    
    data step2;
        set step1;
        do while (condition);
            /* nested do without explicit end */
            value = value + 1;
        end;
    run;
%mend problematic;

/* Test case 6: Edge case with comments and mixed ends */
%macro edge_case;
    /* Comment before proc */
    proc sql noprint;
        /* Complex nested SELECT */
        create table final as
        select t1.*, t2.value
        from (
            select id, sum(amount) as total
            from base_table
            group by id
            having total > 1000
        ) t1
        left join lookup t2
        on t1.id = t2.id;
    quit;
    
    /* Comment between constructs */
    data final_output;
        set final;
        /* Multiple nested conditions */
        if total > 5000 then do;
            category = 'PREMIUM';
            if value > 100 then do;
                subcategory = 'HIGH_VALUE';
            end;
            else do;
                subcategory = 'STANDARD';
            end;
        end;
        else if total > 1000 then do;
            category = 'REGULAR';
        end;
        else do;
            category = 'BASIC';
        end;
        /* Comment before run */
    run;
%mend edge_case;