%macro MBSearches();


	/**** CALL CREDIT ****/
	PROC SQL;
	   CREATE TABLE WORK.QUERY_FOR_SEARCHES AS 
	   SELECT t1.PersonID, 
	          t1.SearchOrgTypeID, 
	          t1.SearchHistoryDate, 
	          t1.SearchReference, 
	          t1.SearchPurposeID
	      FROM BDCC.Searches t1;
	QUIT;

	data work.CC_SEARCH_NORM work.CC_NONCREDSEARCH_NORM;
	set WORK.query_for_searches;

	/*Format applicationdate*/
	format applicationdate date9.;
	applicationdate = datepart(searchhistorydate);
	personID_CC = personID;
	/*Set Bureau to C*/
	bureau="C";
	/*CA is credit application*/
	if SearchPurposeID = 12 then output work.CC_SEARCH_NORM;
	else
	output work.CC_NONCREDSEARCH_NORM;

	keep personID_CC applicationdate bureau;
	run;


	/**** EXPERIAN ****/
	PROC SQL;
	   CREATE TABLE WORK.EXPERIAN_SEARCHES AS 
	   SELECT t1.PersonID, 
	          t2.ApplicationDate, 
	          t2.ApplicationTypeID
	      FROM BDATA.ExCreditApplicationMapping t1
	           INNER JOIN BDATA.ExCreditApplication t2 ON (t1.ExCreditapplicationID = t2.ExCreditApplicationID);
	QUIT;

	data work.EXP_Search_Norm;
	set work.experian_searches;
	bureau="E";
	run;


	/**** JOIN DATA ****/
	PROC SQL;
		create table work.EXP_SEARCH_WITHID as select distinct
			b.NEWID
			,a.applicationdate
			,a.bureau
		from work.EXP_SEARCH_NORM as a
		inner join work.completedsample as b on (a.PersonID = b.PersonID);
	quit;

	proc sql;
		create table work.CC_Search_WithID as select distinct
			b.NEWID
			,a.applicationdate
			,a.bureau
		from work.CC_SEARCH_NORM as a
		inner join work.completedsample as b on (a.personid_cc = b.personid_cc);
	quit;

	data work.ALL_SEARCH_APPEND;
	set work.CC_Search_WithID work.Exp_Search_WithID;
	run;

	proc sql;
		drop table work.exp_search_withid,
		work.cc_search_withid,
		work.QUERY_FOR_SEARCHES,
		work.EXPERIAN_SEARCHES,
		work.CC_SEARCH_NORM,
		work.EXP_SEARCH_NORM;
	quit;


	/**** DE-DUP ****/

	proc sort data=work.all_search_append out=work.sort_search_append;
	by NewID applicationdate;
	run;

	data work.MB_SearchData (drop=temp_applicationdate temp_bureau);
	set work.sort_search_append;
	by NewID applicationdate;
	retain temp_applicationdate temp_bureau;

	if first.NewID then do;
		temp_applicationdate = applicationdate;
		temp_bureau = bureau;
	end;
	else do;
		if temp_applicationdate = applicationdate
			and
				temp_bureau ne bureau
					then delete;
		else do;
		temp_applicationdate = applicationdate;
		temp_bureau = bureau;
		end;
	end;

	run;

	proc sql;
	create table work.ecaps as
	select t1.*,
	t2.debt_code as accountnumber,
	t2.sampledate
	FROM MB_SearchData t1
	INNER JOIN WORK.COMPLETEDSAMPLE_ACC t2 on (t1.NewID = t2.NewID);
	quit;

	proc sort data=ecaps;
	BY accountnumber sampledate;
	run;

	proc sql;
		drop table 
		work.sort_search_append,
		all_search_append,
		MB_SearchData
	;
	quit;

	/**** VARIABLES ****/

	%macro searchvars(type);
		
		NumSearches_&type=0;
		NumSearchesL12M_&type=0;
		NumSearchesL6M_&type=0;
		NumSearchesL3M_&type=0;

		format MostRecentSearch_&type date9.;
		MostRecentSearch_&type='.'d;	

	/*	length hurn_retain_&type $20000;
		hurn_retain_&type='';
		NumAddsSearched_&type=0;*/

	%mend;



	%macro searches(type);

		if	applicationdate<sampledate then NumSearches_&type+1;
		if	intnx('month',sampledate,-12,'s')<applicationdate<sampledate then NumSearchesL12M_&type+1;
		if	intnx('month',sampledate,-6,'s')<applicationdate<sampledate then NumSearchesL6M_&type+1;
		if	intnx('month',sampledate,-3,'s')<applicationdate<sampledate then NumSearchesL3M_&type+1;

		MostRecentSearch_&type=max(MostRecentSearch_&type,applicationdate);

	/*	if indexw(compress(hurn_retain_&type),compress(put(locationid,20.)),'#')=0 then do;
			NumAddsSearched_&type+1;
			hurn_retain_&type=catx('#',hurn_retain_&type,locationid);
		end;*/

	%mend;



	%macro searchcalcs(type);

		if missing(MostRecentSearch_&type)=0 then MonthsRecentSearch_&type = intck('month',MostRecentSearch_&type,sampledate);								else AgeRecentSearch_&type = -999999;

	/*	drop hurn_retain_&type MostRecentSearch_&type;*/

	%mend;

	%let out_data=ecaps_summary;

	%put &out_data;

	data work.&out_data (drop=bureau applicationdate mostrecentsearch_all);
		set ecaps;
		by AccountNumber SampleDate;

		if first.SampleDate then do;
			%searchvars(ALL);
		end;


		retain _all_;

		if applicationdate>SampleDate then delete;

		%searches(ALL);


		if last.SampleDate then do;
			%searchcalcs(All);
			output;
		end;

	run;

	proc sql;
	drop table work.ecaps;
	quit;

%mend;