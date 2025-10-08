/* The code below has been changed so that the shifting is the same regardless of the status of the account*/
%macro BureauDataBlock(DataSet,debtcode,sampledate); 

	%bureaucalcs;

	options sastrace=',,,d' sastraceloc=saslog nostsuffix;

	libname bdata meta library='bureaudata' repname='foundation';
	libname bctl meta library='bureaudata.control' repname='foundation';

	proc sql noprint;
	select controlrunid into :controlrunid
	from bctl.controlrun
	where currentind=1;
	quit;

	data accountmapping;
	set bdata.accountmapping (keep=accountnumber personid currentind);
	where currentind=1;
	drop currentind;
	run;

	data _sample;
	set &dataset (keep=&debtcode &sampledate); 
	where &sampledate>intnx('month',today(),-36,'e');
	rename &debtcode=accountnumber &sampledate=sampledate; 
	run;

	proc sql;
	create table debtcode_pid as
	select	s.accountnumber,
			s.sampledate format=date9.,
			p.PersonID
	from _sample as s
	inner join accountmapping as p on s.accountnumber=p.accountnumber;
	quit;

	data work.caisinfo;
	if _n_=1 then do;
		declare hash cais(dataset:'work.debtcode_pid',multidata:'y');
		cais.definekey('personid'); 
		cais.definedata('sampledate','accountnumber');
		cais.definedone();
	end;
	set bdata.vw_CreditAccountHeaderCurrent (keep=	PersonID CreditAccountHeaderID Opendate CloseDate DefaultDate SourceUpdateDate StatusCode_Current CurrentBalance DefaultBalance AccountTypeDesc AccountTypeCode 
																								CAISSourceCode CreditLimit SpecialInstructionCode SpecialInstructionStartDate);
	where CAISSourceCode is null or CAISSourceCode not in (320,592);
	length accountnumber $10.;
	format sampledate date9.;
	call missing(sampledate,accountnumber);
	f=cais.find();
	do while (f=0);
		output;
		f=cais.find_next();
	end;
	run;


	PROC SQL;
	CREATE TABLE WORK.credaccdistinct AS 
	SELECT DISTINCT 	CREDACCINFO.personid,
									CREDACCINFO.CreditAccountHeaderID,
									CREDACCINFO.opendate,
									CREDACCINFO.closedate,
									CREDACCINFO.defaultdate ,
									CREDACCINFO.SourceUpdateDate
	FROM WORK.caisinfo AS CREDACCINFO;
	QUIT;

	data status;
	set bdata.status;
	where priority<9990;
	run;

	PROC SQL;
	CREATE TABLE WORK.credacchist AS 
	SELECT 	distinct c.personid,
					debtpid.accountnumber, 
					debtpid.sampledate, 
					c.CreditAccountHeaderID,
					datepart(c.opendate) as opendate format=date9.,
					datepart(c.closedate) as closedate format=date9.,
					datepart(c.defaultdate) as defaultdate format=date9.,
					datepart(c.SourceUpdateDate) as SourceUpdateDate format=date9.
					%do i=1 %to 36;
						,ca.Balance&i
					%end;
					%do j=1 %to 36;
						,s&j..Priority as StatusCodePriority&j
					%end;		
	FROM WORK.CREDACCDISTINCT AS c
		inner join bdata.creditaccount ca on c.creditaccountheaderid=ca.creditaccountheaderid and ca.controlrunid=&controlrunid
		%do k=1 %to 36;
			left join status s&k on ca.statusid&k=s&k..statusid
		%end;
		inner join work.debtcode_pid as debtpid on CREDACCDISTINCT.personid=debtpid.personid
	;
	QUIT;

%macro apacs;

		data cc;
		set work.caisinfo (keep=CreditAccountHeaderID personid AccountTypeCode);
		where AccountTypeCode='5';
		run;

		proc sql;
		create table apacscombine as 
		select cc.*
					%do i=1 %to 36;
						,ccp.previousbalance&i as cc_balance&i
					%end;
					%do j=1 %to 36;
						,ccp.amount&j as cc_amount&j
					%end;
		from cc cc
		inner join bdata.creditcardpayment ccp on cc.creditaccountheaderid=ccp.creditaccountheaderid
		;quit;

	%mend apacs;

	%apacs;

	%macro voters;

		PROC SQL;
		CREATE TABLE voters AS 
		SELECT t1.VoterID, 
					  t1.PersonID, 
					  t1.LocationID, 
					  t2.DateRegistered, 
					  t2.DateLeft
		FROM BDATA.VoterMapping t1
		   INNER JOIN BDATA.Voter t2 ON (t1.VoterID = t2.VoterID)
		WHERE t1.currentind = 1;
		QUIT;

		data voterid;
		if _n_=1 then do;
			declare hash h (dataset:'debtcode_pid',multidata:'y');
			h.definekey('personid');
			h.definedata('sampledate','accountnumber');
			h.definedone();
		end;
		set voters;
		length accountnumber $10;
		call missing(sampledate,accountnumber);
		format sampledate date9.;
		f=h.find();
		if dateregistered>sampledate then delete;
		do while (f=0);
			output;
			f=h.find_next();
		end;
		drop f;
		run;

		proc sort data=voterid;
		by accountnumber sampledate descending dateregistered;
		run;

		data work.votersummary;
		set voterid;
		by accountnumber sampledate;

		laglocation=lag(locationid);

		if first.sampledate then do;
			V_CurrentlyRegistered=0;		
			V_TimeAtAddress=0;
			V_NumAddL3M=0;							V_NumAddL6M=0;							V_NumAddL12M=0;									V_NumAddL24M=0;								V_NumAddL48M=0;

			 if missing(dateleft) or dateleft>sampledate then do;
				V_CurrentlyRegistered=1;			
				V_TimeAtAddress=intck('month',dateregistered,sampledate);
			end;
		end;

		retain _all_;

		if first.sampledate or locationid ne laglocation then do;
		if max(dateregistered,dateleft)>intnx('month',sampledate,-3,'s') then V_NumAddL3M+1;
		if max(dateregistered,dateleft)>intnx('month',sampledate,-6,'s') then V_NumAddL6M+1;	
		if max(dateregistered,dateleft)>intnx('month',sampledate,-12,'s') then V_NumAddL12M+1;
		if max(dateregistered,dateleft)>intnx('month',sampledate,-24,'s') then V_NumAddL24M+1;
		if max(dateregistered,dateleft)>intnx('month',sampledate,-48,'s') then V_NumAddL48M+1;
		end;

		if last.sampledate then output;

		drop voterid locationid dateregistered dateleft laglocation;

		run;

	%mend voters;

	%voters;

	proc sql;
	create table caisinfocombined as 
	select 	distinct
				ci.*
				%do i=1 %to 36;
					,ca.balance&i
				%end;
				%do j=1 %to 36;
					,ca.StatusCodePriority&j
				%end;
				%do k=1 %to 36;
					,ap.cc_balance&k
				%end;
				%do l=1 %to 36;
					,ap.cc_amount&l
				%end;
	from work.caisinfo as ci
		inner join credacchist as ca on ci.personid=ca.personid and ci.CreditAccountHeaderID=ca.CreditAccountHeaderID
		left join apacscombine ap on ci.personid=ap.personid and ci.CreditAccountHeaderID=ap.CreditAccountHeaderID
	order by accountnumber, sampledate;
	quit;

options nosymbolgen nomprint nomlogic;

%let hist=36;

/*	data _null_;*/
/*	call symputx('out_data',compress(cats(substr("&dataset",1,27),"_CAIS")));*/
/*	run;*/

	%let out_data=cais_summary;

	%put &out_data;

	data work.cais1;
	set caisinfocombined;
	by accountnumber sampledate;

	length accgroup $50.;
	lastupdatedate=datepart(SourceUpdateDate);
	format lastupdatedate date7.;
	Month=max(intck('month',(sampledate),today()),1);
	if missing(lastupdatedate)=0 then RetroMonth=month-max(intck('month',lastupdatedate,today()),1)+1;
	else RetroMonth=1;

	if datepart(opendate) > (sampledate) then AccountTypeCode='';

	array balance{*} balance1-balance36;
	array bal{&hist};
	array stat{*} statuscodepriority1-statuscodepriority36;
	array status{&hist};
/*	array accstat{*} $1. statuscode1-statuscode36;*/
/*	array accstatus{&hist} $1.;*/

	array cc_balance{*} cc_balance1-cc_balance36;
	array cc_bal{&hist};
	array cc_amount{*} cc_amount1-cc_amount36;
	array cc_amt{&hist};

	select (specialinstructioncode);
		when ('A') specinstrpriority=7;
		when ('C') specinstrpriority=3;
		when ('D') specinstrpriority=1;
		when ('G') specinstrpriority=4;
		when ('I') specinstrpriority=9;
		when ('M') specinstrpriority=8;
		when ('P') specinstrpriority=2;
		when ('Q') specinstrpriority=10;
		when ('R') specinstrpriority=5;
/*		when ('S') specinstrpriority=8;*/
		when ('V') specinstrpriority=6;
		otherwise;
	end;

	select (specialinstructioncode);
    	when ("D", "I", "-1", "V", "R") specinstrpriority1 = 0;
        when ('Q') specinstrpriority1 = 1;
        when ('M') specinstrpriority1 = 2;
        when ('A') specinstrpriority1 = 3;
        when ('P') specinstrpriority1 = 4;
        when ('S') specinstrpriority1 = 5;
        when ('C') specinstrpriority1 = 6;
        when ('G') specinstrpriority1 = 7;
        otherwise; 
     end;
    select (specialinstructioncode);
    	when ("D", "I", "-1", "V", "R") specinstrpriority2 = 0;
        when ('M') specinstrpriority2 = 1;
        when ('A') specinstrpriority2 = 2;
        when ('P') specinstrpriority2 = 3;
        when ('Q') specinstrpriority2 = 4;
        when ('C') specinstrpriority2 = 5;
        when ('S') specinstrpriority2 = 6;
        when ('G') specinstrpriority2 = 7;
        otherwise;
     end;




	do i=1 to min(&hist,36) while (RetroMonth+i-1<=36);
		if (RetroMonth+i-1) > 0 then do;
			bal{i}=balance{RetroMonth+i-1};
			status{i}=stat{RetroMonth+i-1};
	/*		accstatus{i}=accstat{RetroMonth+i-1};*/
			cc_bal{i}=cc_balance{RetroMonth+i-1};
			cc_amt{i}=cc_amount{RetroMonth+i-1};
		end;
		else if Balance1 = 0 then do;
			bal{i}=0;
			status{i}=stat1;
			cc_bal{i}=0;
			cc_amt{i}=0;
		end;
		else do;
			bal{i}=.;
			status{i}=.;
			cc_bal{i}=.;
			cc_amt{i}=.;
		end;
	end;

	if bal4 ^=. and bal3 = . then do i = 1 to 3;
		bal{i}=Bal{4};
		status{i}=status{4};
		cc_bal{i}=cc_bal{4};
		cc_amt{i}=cc_amt{4};
	end;
	else if bal3 ^= . and bal2 = . then do i = 1 to 2;
		bal{i}=Bal{3};
		status{i}=status{3};
		cc_bal{i}=cc_bal{3};
		cc_amt{i}=cc_amt{3};
	end;
	else if bal2 ^= . and bal1 = . then do;
		bal{1}=Bal{2};
		status{1}=status{2};
		cc_bal{1}=cc_bal{2};
		cc_amt{1}=cc_amt{2};
	end;	
		


	if missing(closedate) and missing(defaultdate) then accgroup='active';
	if missing(defaultdate) and 0<datepart(closedate)<=(sampledate) then accgroup='settled';
	if 0<datepart(defaultdate)<=(sampledate) and missing(closedate) then accgroup='default';
	if 0<datepart(defaultdate)<datepart(closedate)<=(sampledate) then accgroup='settled default';
	if missing(defaultdate) and datepart(closedate)>(sampledate) then accgroup='settled was active';
	if datepart(defaultdate)>(sampledate) and missing(closedate) then accgroup='default was active';
	if datepart(defaultdate)>(sampledate) and missing(closedate)=0 then accgroup='settled default was active';
	if 0<datepart(defaultdate)<=(sampledate)<datepart(closedate) then accgroup='settled default was default';

	run;

	data work.&out_data;
	set cais1;
	by accountnumber sampledate;

	array cc_bal{&hist};
	array cc_amt{&hist};

	if first.sampledate then do;

		%createvars(ALL);
		%createvars(Mort);
		%createvars(Tel);
		%createvars(CC);
		%createvars(HP);
		%createvars(Loan);
		%createvars(PDL);
		%createvars(Rev);

		%createvars(P);
		%createvars(SP);
		%createvars(Oth);

		%createvars(CCn);
		%createvars(SPL);
		%createvars(MO);		
		%createvars(UT);		
		%createvars(Lo);		
		%createvars(Com);		
		%createvars(Mor);		
		%createvars(FS);		
		%createvars(Ot);


		%creditcardvars;

	end;

	retain _all_;

	/* ALL ACCOUNTS */
	if missing(AccountTypeCode)=0 then do;
		%calculatedvars(ALL);
	end;

	/* MORTGAGE */
	if AccountTypeCode in (3,16,25) then do;
		%calculatedvars(Mort);
	end;

	/* CREDIT CARD */
	if AccountTypeCode=5 then do;
		%calculatedvars(CC);
		%creditcardcalcs;
	end;

	/* PRIME */
	if AccountTypeCode in (29,0,4,6,26,5,17,15,11,22,19,1,2,8,9,23,7,37,20) then do;
		%calculatedvars(P);
	end;

	/* SUB-PRIME */
	if AccountTypeCode in (61,28) then do;
		%calculatedvars(SP);
	end;

	/* OTHER */
	if AccountTypeCode in (46,18,62,41,40,53,59,60,21,39) then do;
		%calculatedvars(Oth);
	end;

/*	TELCO*/
	if AccountTypeCode in (18,59) then do;
		%calculatedvars(Tel);
	end;

/*	HP*/
	if AccountTypeCode in (1,29) then do;
		%calculatedvars(HP);
	end;

/*	Loan*/
	if AccountTypeCode in (2) then do;
		%calculatedvars(Loan);
	end;

/*	Pay day loan*/
	if AccountTypeCode in (28) then do;
		%calculatedvars(PDL);
	end;

/*	Revolving*/
	if AccountTypeCode in (5,10) then do;
		%calculatedvars(Rev);
	end;



/**** 	New groups ****/

	/*  Credit Card */
	if AccountTypeCode in (5,6,37) then do;
		%calculatedvars(CCn);
	end;

	/*  sub-prime loans */
	if AccountTypeCode in (28,61) then do;
		%calculatedvars(SPL);
	end;

	/*  Mail order */
	if AccountTypeCode in (10,9,8,20) then do;
		%calculatedvars(MO);
	end;

	/*  Utilities */
	if AccountTypeCode in (21,41,40,39,43) then do;
		%calculatedvars(UT);
	end;

	/*  Loan */
	if AccountTypeCode in (2,1,22,19,23,17,26,29) then do;
		%calculatedvars(Lo);
	end;

	/*  Telco */
	if AccountTypeCode in (18,59,53) then do;
		%calculatedvars(Com);
	end;

	/*  Mortgage */
	if AccountTypeCode in (3,25,33,30,16,35,31,32,14) then do;
		%calculatedvars(Mor);
	end;

	/*  FS */
	if AccountTypeCode in (11,27,11,0) then do;
		%calculatedvars(FS);
	end;

	/*  Other */
	if AccountTypeCode in (7,45,60,46,50,62,4) then do;
		%calculatedvars(Ot);
	end;




	if last.sampledate then do;

		%summarycalcs(ALL);
		%summarycalcs(Mort);
		%summarycalcs(CC);
		%summarycalcs(HP);
		%summarycalcs(Tel);
		%summarycalcs(Loan);
		%summarycalcs(PDL);
		%summarycalcs(Rev);

		%summarycalcs(P);
		%summarycalcs(SP);
		%summarycalcs(Oth);

		%summarycalcs(CCn);		
		%summarycalcs(SPL);		
		%summarycalcs(MO);		
		%summarycalcs(UT);		
		%summarycalcs(Lo);		
		%summarycalcs(Com);		
		%summarycalcs(Mor);		
		%summarycalcs(FS);		
		%summarycalcs(Ot);


		%ccsummary;
		
		BalAccsOpenG12M_ExclMort = BalActAccsOpenG12M_ALL - BalActAccsOpenG12M_Mort;													
		BalAccs_ExclMort = BalActAccs_ALL - BalActAccs_Mort;
/**/
/*		if TotalDebt_ALL>0 then PctDebtSubPrime = TotalDebt_SP / TotalDebt_ALL;				else PctDebtSubPrime=-99999;*/
/*		if TotalDebt_ALL>0 then PctDebtPrime = TotalDebt_P / TotalDebt_ALL;						else PctDebtPrime=-99999;*/
/**/
/*		if (TotalDebt_ALL-TotalDebt_Mort)>0 then PctDebtExMortSubPrime = TotalDebt_SP / (TotalDebt_ALL-TotalDebt_Mort);				else PctDebtExMortSubPrime=-99999;*/
/*		if (TotalDebt_ALL-TotalDebt_Mort)>0 then PctDebtExMortPrime = TotalDebt_P / (TotalDebt_ALL-TotalDebt_Mort);						else PctDebtExMortPrime=-99999;*/


		drop 	creditaccountheaderid opendate closedate defaultdate statuscode_current currentbalance defaultbalance specialinstructioncode specialinstructionstartdate creditlimit accounttypecode accounttypedesc caissourcecode
					balance1-balance36 statuscodepriority1-statuscodepriority36 bal1-bal36 status1-status36 lastupdatedate month retromonth MonthClosed i f sourceupdatedate accgroup 
					cc_bal: cc_am: 
					_temp: specinstrpriority
		;
		output;
	
	end;

	run;

/*	data _null_;*/
/*	call symputx('out_pid',compress(cats(substr("&dataset",1,27),"_PID")));*/
/*	run;*/
		
	%let out_pid=pids;

	%put &out_pid;

	data &out_pid;
	set work.debtcode_pid;
	run;

	proc sql;
	drop table	accountmapping
						,_sample
						,caisinfo
						,credaccdistinct
						,credacchist
/*						,caisinfocombined*/
						,debtcode_pid

						  ,cc
						  ,apacscombine
						  ,voters
						  ,voterid
/*						  ,cais1*/
	;
	quit;

%mend;