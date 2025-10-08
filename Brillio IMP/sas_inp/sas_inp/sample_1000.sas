/*libname xo '\\client_name2.local\shares\SASWORK\SASUSERS\pyoung\Crossover';*/
libname pms meta library='pms';
libname scor meta library='pricing_sandpit.scorecards';
libname p2scflow meta library='pinsys_snapshot';
libname scor meta library='pricing_sandpit.scorecards';
libname trcmgmt meta library='tracemanagement';
libname ons '\\client_name2.local\shares\SASWORK\DATA\ONS';
libname letters meta library='letters';
libname cssms meta library='collstreamsms';
libname bdata meta library='bureaudata';
libname trcmgmt meta library='tracemanagement';
libname dwhdw meta library='datawarehouse.dw';

%include '\\client_name2.local\shares\SASWork\Variable Creation\Macros\PY XO\xoCalcs.sas';

%let benchmarklist=14050,14051;

%macro benchmark;

	proc sql noprint;
		create table benchmark_pms as 
		select t1.portfolioid
					,t1.assetid
					,t1.accountnumber
					,t1.debtcode
					,t1.homephone
					,t1.businessphone
					,t1.balance
					,t1.postcode						
					,p.collectionsname
					,p.type
					,p.name
					,p.startdate
					,p.'tdx segment'n as segment
					,p.sector
					,p.client
					,p.portfoliocategory
					,cp.combinedportfolioname
		from pms.tblassetdetailscrosstab t1
			inner join pms.tblportfolios p on t1.portfolioid=p.portfolioid
			inner join pms.tblcombinedjoin cj on p.portfolioid=cj.portfolioid
			inner join pms.tblcombinedportfolios cp on cj.combinedportfolioid=cp.combinedportfolioid
		where t1.portfolioid in (&benchmarklist)
		;
		/*Selecting Collection names to Variable Separated by ',' from Benchmark_pms*/
		select distinct cats("'",collectionsname,"'")
		into :portfolios separated by ','
		from benchmark_pms
		;quit;

		%put &portfolios;

		proc sql noprint;
		create table Joint_Matter as
		select 	a.debt_code as debtcode,
				b.matter_type,
				max(case when a.dr_rectype="S" then 1
					else . end) as Joint_Flag,
				max(case when a.dr_rectype="D" and b.matter_type in ('PNI','PNM','PNS','PRI') then 1
					else . end) as Matter_Type_Flag
		from p2scflow.debtor a
			inner join p2scflow.debt b on a.debt_code=b.debt_code
		where a.dr_rectype in ("S","D") and b.client_code in (&portfolios.)
		group by 1,2
		;quit;

		proc sql;
		create table benchmark_cflow as 
		select 	d.debt_code
					,df.dfdefbalance as defaultbalance
					,df.dfdefdate as defaultdate
					,d.dt_jmtdat as judgementdate
					,d.dt_pdtodate as paidtodate
					,df.dfdefdate as chargeoffdate --Brillio - Why dfdefdate is used as Default and Chargeoffdate?
		from p2scflow.debt d
		inner join p2scflow.defdetails df on d.debt_code=df.debt_code
		where d.client_code in (&portfolios)
		;
		create table wh as 
		select acc.accountnumber
					,case when pc.country is missing then 'Unknown' 
					      else pc.country end as LegalJurisdiction
					,acc.creditagreementdate as clientstartdate 
					,cust.dateofbirth 
					,acc.LastPaymentToClientDate 
					,acc.originalbalance
					,(acc.LastPaymentToClient/(sum(acc.originalbalance,acc.LastPaymentToClient)))*100 as LastPaymentToClientAsPctOfBal
	                ,case when acc.defaultbalance in (0, .) then . 
	                	else (acc.LastPaymentToClient/acc.defaultbalance)*100 end as LastPaymentToClientAsPctOfDef
					,acc.LastPaymentToClient as lastpaymentamount 
					,acc.defaultbalance
					,acc.defaultdate
					,acc.judgementdate
					,acc.bookondate			
		from dwhdw.dim_account acc
		  left join
		    dwhdw.bridge_customeraccount br
	      on acc.accountkey=br.accountkey and br.currentrecord='Y'
		  left join
		    dwhdw.dim_customer cust
		  on br.customerkey=cust.customerkey and cust.currentrecord='Y'
		  left join 
		    dwhdw.dim_postcode pc
		  on cust.postcode=pc.postcode
		where acc.caseflowportfolioname in (&portfolios)
		;
		create table benchmark as 
		select p.*
					,c.paidtodate
					,c.defaultbalance
					,wh.LegalJurisdiction
					,wh.dateofbirth as dob
					,wh.LastPaymentToClientDate as lastdatepaid
					,wh.lastpaymentamount as lastamountpaid
					,wh.defaultdate
					,wh.defaultdate as chargeoffdate
					,wh.judgementdate
					,wh.clientstartdate as accountopendate
					,wh.bookondate
					,j.Joint_Flag
					,j.Matter_Type_Flag
		from benchmark_pms p
		left join benchmark_cflow c on p.debtcode=c.debt_code
		left join wh on p.debtcode=wh.accountnumber
		left join Joint_Matter j on p.debtcode=j.debtcode
		;quit;

		%let numerics=0123456789;

		data benchmark2;
		set benchmark;

		if StartDate ne '' then
		StartDate2 = datepart(StartDate);
		format StartDate2 date9.;

		if Balance ne '' then
		Balance2 = input(Balance, 10.);

		run;

		PROC SQL;
		   CREATE TABLE benchmarkdata AS 
		   SELECT t1.PortfolioID, 
		          t1.AssetID, 
		          t1.AccountNumber, 
		          t1.DebtCode, 
		          t1.Balance2 AS Balance, 
		          t1.StartDate2 as startdate, 
		          t1.defaultbalance, 
		          t1.defaultdate, 
		          t1.AccountOpenDate, 
		          t1.DOB, 
		          t1.ChargeOffDate, 
		          t1.LastDatePaid, 
				  t1.JudgementDate,
		          t1.LastAmountPaid, 
		          t1.HomePhone, 
		          t1.BusinessPhone, 
		          t1.CollectionsName, 
		          t1.Type, 
		          t1.Name, 
		          t1.segment, 
		          t1.Sector, 
		          t1.Client, 
		          t1.PortfolioCategory, 
		          t1.CombinedPortfolioName,
				  t1.postcode,
				  t1.LegalJurisdiction,
				  t1.bookondate,
				  t1.Joint_Flag,
				  t1.Matter_Type_Flag
		      FROM WORK.BENCHMARK2 t1;
		QUIT;

%mend;

%macro linkedaccs;

	proc sql noprint;
		create table chosenportfolio1 as
		select portfolioid,
					assetid,
					debtcode,
				   	name,
					startdate,
					collectionsname,
					balance,
					 segment 
		from benchmarkdata
		;
		select distinct cats("'",collectionsname,"'") into :portfolioname separated by ","
		from chosenportfolio1;
	quit;

	%put &portfolioname;

	proc sql noprint;
	select min(startdate)
				,max(startdate)
	into :mindate
			,:maxdate
	from chosenportfolio1
	;quit;

	proc sql;
	create table accmapping as 
	select a.accountnumber
				,a.personid
				,a.currentind
				,datepart(a.startdate) as startdate format=date9.
				,datepart(a.enddate) as enddate format=date9.
				,p.sourcekey as pin
				,l.sourcekey as lin
	from bdata.AccountMappingALLRecords a 
	inner join bdata.PersonALLRecords p on a.personid=p.personid
	inner join bdata.LocationALLRecords l on a.locationid=l.locationid
	where a.enddate>&mindate*60*60*24 and a.startdate<(&maxdate+30)*60*60*24
	order by a.accountnumber, a.startdate
	;quit;

	proc sort data=accmapping out=accmapping2;
	by accountnumber;
	run;

	proc sql;
	create table accountspid as 
	select p.*
				,a.personid
				,a.pin
				,a.lin
	from CHOSENPORTFOLIO1 (rename=(debtcode=accountnumber)) p
	left join accmapping2 a on p.accountnumber=a.accountnumber and (a.startdate-30)<=p.startdate<=a.enddate
	;quit;

	proc sort data=accountspid nodupkey;
	by accountnumber;
	run;

	proc sql;
	create table previouspurchases as
	select adc.debtcode
				,p.startdate
	from pms.tblassetdetailscrosstab adc
	inner join pms.tblportfolios p on adc.portfolioid = p.portfolioid
	where p.startdate<(&maxdate*60*60*24)
	and adc.debtcode ne ''
	;quit;

	proc sql;
	create table prevpurchpid as 
	select p.*
				,a.personid
				,a.pin
				,a.lin
				,a.startdate as pidstart
				,a.enddate as pidend
				,d.rep_code
				,d.dt_rep_init_date
				,d.matter_type
				,d.dstb
	from previouspurchases (rename=(debtcode=accountnumber)) p
	inner join accmapping a on p.accountnumber=a.accountnumber
	left join p2scflow.debt d on p.accountnumber=d.debt_code
	;quit;

	proc sql;
	create table linkedaccs as 
	select a.*
				,p.accountnumber as linked_accountnumber
				,datepart(p.startdate) as linked_startdate format=date9.
	from accountspid a 
	left join prevpurchpid p on a.pin=p.pin and p.pidstart<=a.startdate<=p.pidend and datepart(p.startdate)<a.startdate
		and ((p.rep_code like '9%' and datepart(p.dt_rep_init_date)>=intnx('year',a.startdate,-3,'s')) or p.rep_code not like '9%')
		and ((p.matter_type='STB' and datepart(p.dstb)>=intnx('year',a.startdate,-3,'s')) or p.matter_type ne 'STB')
	;quit;

	proc sql;
	create table benchmark_linked as 
	select accountnumber
				,pin
				,lin
				,personid
				,linked_accountnumber
	from linkedaccs
	;quit;

%mend;

%macro data;
	
	proc sql noprint;
		create table benchmark as
		select  
					 bd.debtcode as accountnumber,
				   	 bd.name,
					 bd.startdate*60*60*24 as startdate format=datetime19.,
					 bd.startdate*60*60*24 as purchasedate format=datetime19.,
					 bd.balance,
					 bd.segment,
					 bd.sector,
					 bd.type,
					 bd.LastDatePaid, 
	          		 bd.LastAmountPaid,
					 bd.homephone,
					 bd.businessphone,
					 bd.postcode,
					 case when length(compress(bd.postcode))=5 then upcase(substr(compress(bd.postcode),1,3))
					 	when length(compress(bd.postcode))=6 then upcase(substr(compress(bd.postcode),1,4))
						when length(compress(bd.postcode))=7 then upcase(substr(compress(bd.postcode),1,5))
						else 'Unknown' end as postcode_sector,
					 bd.accountopendate,
					 bd.chargeoffdate,
					 bd.defaultbalance,
					 bd.dob,
					 bd.judgementdate,
					 bd.client,
					 bd.collectionsname,
					 bd.Joint_Flag,
					 bd.Matter_Type_Flag,
/*						 bd.paidtodate,*/
					 0 as paidtodate,
					 'Benchmark' as datatype length=20,
					 bl.pin,
					 bl.personid as personid_old,
					 am.personid,
					 bl.linked_accountnumber
		from benchmarkdata bd
		left join benchmark_linked bl on bd.debtcode=bl.accountnumber
		left join bdata.accountmapping (where=(currentind=1)) am on bd.debtcode=am.accountnumber
/*		where bxo.group not in ('08.New','09.Zero PIN')*/
		where missing(bl.linked_accountnumber)=0 
	;
	select distinct cats("'",name,"'") into :portfolioname separated by ","
		from benchmark
	;
	select distinct cats("'",collectionsname,"'") into :portfoliocode separated by ","
		from benchmark
	;
	select distinct(cats("'",upcase(cp.combinedportfolioname),"'")) into :combinedportfolio separated by ","
		from benchmark b
		inner join pms.tblportfolios p on b.collectionsname=p.collectionsname
		inner join pms.tblcombinedjoin cj on p.portfolioid=cj.portfolioid
		inner join pms.tblcombinedportfolios cp on cj.combinedportfolioid=cp.combinedportfolioid
	;
	;quit;

	proc sql inobs=1 noprint;
	select sector into :sector
	from benchmark
	;quit;
	%if &sector=Home Credit %then %let sector3=Financial Services;
	%else %if &sector=Utilities %then %let sector3=Communications;
	%else %let sector3=&sector;

	data alldata;
	set benchmark; 
	run; 

	%put &portfolioname &sector;

	%global mindate maxdate maxdateM6;
	proc sql noprint;
	select datepart(min(startdate))
				,datepart(max(startdate))
				,intnx('month',datepart(max(startdate)),6,'s') 
	into :mindate
			,:maxdate
			,:maxdateM6
	from alldata
	;quit;

%mend data;

%macro linkeddata;

	proc sql;
	create table paytrans as 
	SELECT distinct 
		da.AccountNumber as debt_code,
		f.PaymentDateKey,
		d.date*60*60*24 as paymentdate format=datetime19.,
		d.date*60*60*24 as dt_tx_date format=datetime19.,
		f.PaymentAmount as tx_amount,
		l.transactioncode as tran_code
	FROM dwhdw.fact_tx_Payment f
	INNER JOIN dwhdw.dim_Account da ON da.AccountKey = f.AccountKey
	INNER JOIN dwhdw.lookupref_Caseflow_Transactions l ON l.TransactionKey = f.TransactionKey
	inner join dwhdw.dim_date d on f.paymentdatekey=d.datekey
	WHERE l.Payment = 1
		and d.date <= &maxdateM6
		and da.AccountNumber in (select distinct linked_accountnumber from alldata 
											where missing(linked_accountnumber)=0
											outer union corr
											select distinct accountnumber as linked_accountnumber from alldata)
	ORDER BY 
		da.AccountNumber,
		f.PaymentDateKey
	;quit;

/*		Insolvency bureau*/
	proc sql;
	create table work.Insolvent_HP as
	select	personid,
				publicinformationdate as InsolventDate format=datetime16.
	from bdata.vw_PublicInformationCurrent
	where publicinformationtypecode in ('3','14','25','29','31','33','38','40','42','44','46','47','48','49','53','54','60','61','64','70','72','77','78')
	;quit;

/*		Previously selected for litigation*/
	proc sql;
	create table previousselectionstemp as 
	select debtcode as debt_code
				,selectiondate as dt_tx_date format=datetime19.
	from scor.litigationselections
	;
	create table work.LitigationHistory as
	select	debt_code,
				dt_tx_date as LitigationActionDate format=datetime9.,
				(case when upcase(tran_code) in ('FF1011','FF1010','FF1081','FF1110') then dt_tx_date
					else . end) as LitigationClaimDate format=datetime9.
	from p2scflow.debt_trans
	where tran_code like 'FF%' or tran_code like 'FC%'
	;quit;

/*		Fraudulant Accounts*/
	proc sql;
	create table Fraud as
	select 	debt_code,
			dt_tx_date as Fraud_Date format=datetime9.
	from p2scflow.debt_trans
	where tran_code in ('MO2647','MO2987','MO3126','MO3211','MO3313','MO3344','MO3754','MO3773','MO3870','MO3933','MO3944','MO3958')
		and dt_tx_date ge &mindate.*60*60*24
	;quit;

/*		Disposable income*/
	proc sql;
	create table disposableincome as
	select 	debt_code,
			disposableincome,
			budgetdatetime as date format=datetime9.
	from p2scflow.budget
	where budgetdatetime ge &mindate.*60*60*24
	;quit;

/*		Postcode*/
	proc import datafile='\\lwsvna-02\mi_reporting\Modelling\Scorecards\SAS scoring\Postcodeinfo.xls'
	out=postcodeinfo
	dbms=excelcs replace;
	SHEET="sheet1"; 
	run;

	data postcodesector;
	set WORK.POSTCODEINFO;
	length postsector $40.;
	PostSector=compress('post sector'n);
	run;

	proc sort data=postcodesector nodupkey;
	by postsector;
	run;

/*		ECAPS*/
	data _null_;
	call symputx('d6m',intnx('month',&mindate,-6,'s'));
	run;

/*		Stop list*/
	proc sql;
	create table stoplist as 
	select debtcode
	from letters.exclusionlist
	;quit;

	%macro status;

		proc datasets library=work nolist;
		delete linkedstatus linkedplans;
		run;

		proc sql;
		create table distinctmonth as 
		select distinct intnx('month',datepart(startdate),0,'e') as month format=date9.
		from alldata
		;quit;

		data _null_;
		set distinctmonth end=last;
		call symputx(cats('monthfilter',_n_),month);
		call symputx(cats('monthfilter2_',_n_),put(month,yymmddn8.));
		call symputx(cats('monthfilterdt',_n_),month*60*60*24);
		if last then call symputx('benchmonths',_n_);
		run;
		%put &benchmonths;

		%do i=1 %to &benchmonths;

			data _null_;
			call symputx('startdate',intnx('month',&&monthfilter&i,0,'b'));
			call symputx('enddate',intnx('month',&&monthfilter&i,0,'e'));
			run;

			proc sql;
			create table linkedstatus_temp&i as 
			select  
			    da.AccountNumber										
			    ,das.AccountStatusCode
			    ,das.AccountStatusDescription
			    ,das.OperationalStatusCategory
			    ,f.DateKey	
				,f.TimeKey	
				,f.enddate
				,f.endtime	
			    ,da.OriginalBalance as originalbalanceamount							
			    ,f.CurrentBalance							
			    ,p.ClientSector
			    ,da.CurrentContactStrategyCode as CurrentContactStategyCode					
			    ,aa.dateofbirth
			    ,aa.PhoneNumber1 as homephonenumber
			    ,aa.PhoneNumber2 as workphonenumber
			    ,aa.PhoneNumber3 as mobilephonenumber
				,'' as phonenumber4
				,'' as phonenumber5
				,'' as phonenumber6
			    ,aa.PostCode							
			    ,CASE WHEN da.StatuteBarredDate IS missing OR da.StatuteBarredDate > today() THEN '' ELSE 'StatuteBarred' END AS StatuteBarredFlag	
			    ,p.PortfolioPurchaseDate as PortfolioPurchaseDate_temp
				,p.portfoliocode
				,&&monthfilter&i as date format=date9.
			FROM dwhdw.fact_tx_AccountStatus f
			inner JOIN dwhdw.dim_Account da ON da.AccountKey = f.AccountKey
			inner JOIN dwhdw.dim_AccountStatus das ON das.AccountStatusKey = f.AccountStatusKey_Current
			inner JOIN dwhdw.dim_Portfolio p ON p.PortfolioKey = f.PortfolioKey
			left JOIN dwhdw.bridge_CustomerAccount ca on ca.AccountKey = da.AccountKey and ca.RowStartDate<=&&monthfilterdt&i<ca.RowEndDate
			left JOIN dwhdw.dim_Customer aa ON aa.CustomerKey = ca.CustomerKey 
			where f.datekey<=&&monthfilter2_&i<f.enddate 
			and p.portfoliokey>0
			;
			create table linkedstatus_&i as 
			select s.*
						,s.PortfolioPurchaseDate_temp*60*60*24 as PortfolioPurchaseDate format=datetime19.
			from linkedstatus_temp&i s
			where s.accountnumber in 	
				(select distinct linked_accountnumber
				from alldata
				where &startdate<=datepart(startdate)<=&enddate 
				and missing(linked_accountnumber)=0
				outer union corr
				select distinct accountnumber as linked_accountnumber
				from alldata
				where &startdate<=datepart(startdate)<=&enddate 
				and datatype like 'Benchmark%')
			;quit;

			proc append base=linkedstatus
			data=linkedstatus_&i
			force nowarn;
			run;

			proc datasets library=work nolist;
			delete linkedstatus_&i linkedstatus_temp&i;
			run;

			/*		In progress plans*/
			PROC SQL;
		   CREATE TABLE WORK.linkedplans_temp&i AS 
		   SELECT 
		          t1.debt_code, 
		          t1.iphcreationdate, 
		          t1.iphstatus, 
		          t1.iphcompletiondate, 
		          t1.tcfrecordmodified,
				  &&monthfilter&i as date format=date9.
		      FROM P2SCFLOW.instplanheader t1
		      WHERE ( t1.iphcreationdate <= &&monthfilter&i*60*60*24 AND t1.iphstatus = 'In Progress' ) 
				OR ( t1.iphcreationdate <= &&monthfilter&i*60*60*24 <= t1.tcfrecordmodified )
		      ORDER BY t1.iphcreationdate;
			QUIT;

			proc append base=linkedplans
			data=linkedplans_temp&i
			force nowarn;
			run;

			proc datasets library=work nolist;
			delete linkedplans_temp&i;
			run;

		%end;

	%mend;

	%status;

	proc sql;
	create table RPCs as 
	select t1.accountnumber
				,t0.activitydate as activitytimestamp
				,t2.method
				,t2.direction
				,t2.connect
				,t2.rpc
				,t2.closure
	from dwhdw.fact_tx_AccountActivity t0
	inner join dwhdw.dim_account t1 on t0.accountkey=t1.accountkey
	inner join dwhdw.dim_activity t2 on t0.activitykey=t2.activitykey
	where t2.method='Call'
	and t0.activitydate<=(&maxdateM6*60*60*24)
	and t2.rpc='Yes'
	;
	create table linkedrpc as 
	select distinct 
				r.*
				,datepart(r.activitytimestamp) as date format=date9.
				,l.personid
	from rpcs r
	inner join alldata l on r.accountnumber=l.linked_accountnumber 
	where missing(l.linked_accountnumber)=0
	order by l.personid, r.activitytimestamp
	;
	drop table rpcs
	;quit;

	proc sort data=linkedrpc nodupkey;
	by personid date;
	run;

	data _null_;
	call symputx('minstart_m1',intnx('month',&mindate,-1,'b')*60*60*24);
	run;

	proc sql;
	create table trace as 
	select compress(t.tcfreferencenumber,,'dk') as tcfreferencenumber
				,t.tcfclientcode
				,t.tcflivingasstated
				,t.tcftraceresulttype
				,r.trtDescription
				,t.tcfhomeowner
				,t.tcftimestamp
				,min(tcftimestamp) as firsttrace format=datetime19.
				,case when tcfLivingasStated in (-1,1) and tcfTraceResultType in(4,5,9,10) then '1.LAS'
					when tcfLivingasStated = 0  and tcfTraceResultType in(4,5,9,11) then '2.NewAddress'
					else '3.Neg' 
					end as TraceType
				,t.tcfNewPostCode
	from trcmgmt.tcaseflowdataaudit t
	left join trcmgmt.tresulttype r on t.tcftraceresulttype=r.trtTypeID
	where tcfactionperformed='Inserted'
		and t.tcftimestamp>=&minstart_m1
	group by calculated tcfreferencenumber
	having calculated firsttrace=t.tcftimestamp
	;quit;

	proc sort data=alldata out=alldatadedup nodupkey;
	by accountnumber datatype;
	run;
/*Didnt Analyzed*/
	proc sql;
	create table linkeddata as 
	select distinct 
				l.*
				,p.*
				,p2.*
				,s.*
				,s2.*
				,r.*
				,i.*
				,lit.*
				,lit2.*
				,litact.*
				,inc.disposableincome
				,f.Fraud_Date
				,post.town
				,post.country
				,case when missing(stop.debtcode)=0 then 1 else 0 end as linked_stoplist
				,case when missing(plan.accountnumber)=0 then 1 else 0 end as linked_activeplan
				,plan2.PaymentPlanStatus
				,t.tracetype as BenchLimaTraceResult
				,t.tcfNewPostCode as BenchLimaTracedPcode
	from alldata l
	left join postcodesector post on l.postcode_sector=post.postsector
	left join trace t on l.accountnumber=t.tcfreferencenumber and l.datatype='Benchmark'
	left join	
		(select l.accountnumber
					,l.datatype
					,p.debt_code as linked_accountnumber
					,sum(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' then p.tx_amount end) as linked_internalpay
					,sum(case when p.tran_code like 'DR5%' then p.tx_amount end) as linked_externalpay
					,sum(case when p.tran_code like 'DR4%' then p.tx_amount end) as linked_directpay

					,max(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' then p.paymentdate end) as linked_internalpaydate format=datetime19.
					,max(case when p.tran_code like 'DR5%' then p.paymentdate end) as linked_externalpaydate format=datetime19.
					,max(case when p.tran_code like 'DR4%' then p.paymentdate end) as linked_directpaydate format=datetime19.

					,min(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' then p.paymentdate end) as linked_mininternalpaydate format=datetime19.
					,min(case when p.tran_code like 'DR5%' then p.paymentdate end) as linked_minexternalpaydate format=datetime19.
					,min(case when p.tran_code like 'DR4%' then p.paymentdate end) as linked_mindirectpaydate format=datetime19.

					,count(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' then p.paymentdate end) as linked_countinternalpay 
					,count(case when p.tran_code like 'DR5%' then p.paymentdate end) as linked_countexternalpay
					,count(case when p.tran_code like 'DR4%' then p.paymentdate end) as linked_countdirectpay 

					,sum(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' and p.paymentdate>intnx('dtmonth',l.startdate,-1,'s') then p.tx_amount end) as linked_internalpayL1M
					,sum(case when p.tran_code like 'DR5%' and p.paymentdate>intnx('dtmonth',l.startdate,-1,'s') then p.tx_amount end) as linked_externalpayL1M
					,sum(case when p.tran_code like 'DR4%' and p.paymentdate>intnx('dtmonth',l.startdate,-1,'s') then p.tx_amount end) as linked_directpayL1M

					,sum(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' and p.paymentdate>intnx('dtmonth',l.startdate,-3,'s') then p.tx_amount end) as linked_internalpayL3M
					,sum(case when p.tran_code like 'DR5%' and p.paymentdate>intnx('dtmonth',l.startdate,-3,'s') then p.tx_amount end) as linked_externalpayL3M
					,sum(case when p.tran_code like 'DR4%' and p.paymentdate>intnx('dtmonth',l.startdate,-3,'s') then p.tx_amount end) as linked_directpayL3M

					,sum(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' and p.paymentdate>intnx('dtmonth',l.startdate,-6,'s') then p.tx_amount end) as linked_internalpayL6M
					,sum(case when p.tran_code like 'DR5%' and p.paymentdate>intnx('dtmonth',l.startdate,-6,'s') then p.tx_amount end) as linked_externalpayL6M
					,sum(case when p.tran_code like 'DR4%' and p.paymentdate>intnx('dtmonth',l.startdate,-6,'s') then p.tx_amount end) as linked_directpayL6M

					,sum(case when p.tran_code like 'DR3%' and p.tran_code not like 'DR33%' and p.paymentdate>intnx('dtmonth',l.startdate,-12,'s') then p.tx_amount end) as linked_internalpayL12M
					,sum(case when p.tran_code like 'DR5%' and p.paymentdate>intnx('dtmonth',l.startdate,-12,'s') then p.tx_amount end) as linked_externalpayL12M
					,sum(case when p.tran_code like 'DR4%' and p.paymentdate>intnx('dtmonth',l.startdate,-12,'s') then p.tx_amount end) as linked_directpayL12M

					,max(case when p.tran_code not like 'DR33%' and (p.tran_code not like 'DR4%' or p.tran_code in ('DR4000','DR4001','DR4050','DR4060','DR4070')) then p.paymentdate end) as linked_paydatelit 

		from paytrans p
		inner join alldata l on l.linked_accountnumber=p.debt_code and p.paymentdate<l.startdate
		group by 1,2,3) p on l.accountnumber=p.accountnumber and l.linked_accountnumber=p.linked_accountnumber and l.datatype=p.datatype

	left join
		(select l.accountnumber
					,l.datatype
					,sum(p.tx_amount) as client_namePayments
					,max(p.paymentdate) as client_nameLastPayDate format=datetime19.
		from paytrans p
		inner join alldatadedup l on l.accountnumber=p.debt_code and p.paymentdate<l.startdate
		where l.datatype ne 'SaleFile'
		group by 1,2) p2 on l.accountnumber=p2.accountnumber and l.datatype=p2.datatype

	left join 
		(select l.accountnumber
					,l.datatype
					,s.accountnumber as linked_accountnumber
					,s.accountstatuscode as linked_statuscode
					,s.accountstatusdescription as linked_statusdescription
					,s.operationalstatuscategory as linked_statuscategory
					,s.originalbalanceamount as linked_origbalance
					,s.currentbalance as linked_currbalance
					,s.clientsector as linked_sector
					,s.CurrentContactStategyCode as linked_strategycode
					,s.dateofbirth as linked_DOB
					,s.homephonenumber as linked_homephone
					,s.workphonenumber as linked_workphone
					,s.mobilephonenumber as linked_mobilephone
					,s.phonenumber4 as linked_phone4
					,s.phonenumber5 as linked_phone5
					,s.phonenumber6 as linked_phone6
					,s.postcode as linked_postcode
					,s.StatuteBarredFlag as linked_StatuteBarredFlag 
					,s.portfoliopurchasedate as linked_startdate
		from linkedstatus s
		inner join alldata l on l.linked_accountnumber=s.accountnumber and intnx('month',datepart(l.startdate),0,'e')=s.date) s on l.accountnumber=s.accountnumber 
																																																									and l.linked_accountnumber=s.linked_accountnumber and l.datatype=s.datatype
	left join
		(select l.accountnumber
					,l.datatype
					,p.debt_code as linked_accountnumber
		from linkedplans p
		inner join alldata l on l.linked_accountnumber=p.debt_code and intnx('month',datepart(l.startdate),0,'e')=p.date) plan on l.accountnumber=plan.accountnumber 
																																																									and l.linked_accountnumber=plan.linked_accountnumber and l.datatype=plan.datatype
	left join
		(select l.accountnumber
					,l.datatype
					,p.iphstatus as PaymentPlanStatus
		from linkedplans p
		inner join alldata l on l.accountnumber=p.debt_code and intnx('month',datepart(l.startdate),0,'e')=p.date
		where l.datatype like 'Benchmark%') plan2 on l.accountnumber=plan2.accountnumber and l.datatype=plan2.datatype

	left join	
		(select l.accountnumber
					,l.datatype
					,s.accountstatuscode
					,s.currentbalance
					,s.AccountStatusDescription
					,s.OperationalStatusCategory
			from alldata l
			inner join linkedstatus s on l.accountnumber=s.accountnumber and intnx('month',datepart(l.startdate),0,'e')=s.date
			where l.datatype like 'Benchmark%') s2 on l.accountnumber=s2.accountnumber and l.datatype=s2.datatype

	left join 
		(select l.accountnumber
					,l.datatype
					,r.accountnumber as linked_accountnumber
					,sum(case when r.rpc='Yes' then 1 end) as linked_RPCs
					,sum(case when r.rpc='Yes' and r.direction='Outbound' then 1 end) as linked_OutRPCs
					,sum(case when r.rpc='Yes' and r.direction='Inbound' then 1 end) as linked_InRPCs

					,sum(case when r.rpc='Yes' and r.direction='Outbound' and r.activitytimestamp>intnx('dtmonth',l.startdate,-3,'s') then 1 end) as linked_OutRPCsL3M
					,sum(case when r.rpc='Yes' and r.direction='Outbound' and r.activitytimestamp>intnx('dtmonth',l.startdate,-6,'s') then 1 end) as linked_OutRPCsL6M
					,sum(case when r.rpc='Yes' and r.direction='Outbound' and r.activitytimestamp>intnx('dtmonth',l.startdate,-12,'s') then 1 end) as linked_OutRPCsL12M

					,sum(case when r.rpc='Yes' and r.direction='Inbound' and r.activitytimestamp>intnx('dtmonth',l.startdate,-3,'s') then 1 end) as linked_InRPCsL3M
					,sum(case when r.rpc='Yes' and r.direction='Inbound' and r.activitytimestamp>intnx('dtmonth',l.startdate,-6,'s') then 1 end) as linked_InRPCsL6M
					,sum(case when r.rpc='Yes' and r.direction='Inbound' and r.activitytimestamp>intnx('dtmonth',l.startdate,-12,'s') then 1 end) as linked_InRPCsL12M

					,max(case when r.rpc='Yes' then r.activitytimestamp end) as linked_MaxRPC format=datetime19.
					,max(case when r.rpc='Yes' and r.direction='Inbound' then r.activitytimestamp end) as linked_MaxInRPC format=datetime19.
					,max(case when r.rpc='Yes' and r.direction='Outbound' then r.activitytimestamp end) as linked_MaxOutRPC format=datetime19.

					,min(case when r.rpc='Yes' then r.activitytimestamp end) as linked_MinRPC format=datetime19.
					,min(case when r.rpc='Yes' and r.direction='Inbound' then r.activitytimestamp end) as linked_MinInRPC format=datetime19.
					,min(case when r.rpc='Yes' and r.direction='Outbound' then r.activitytimestamp end) as linked_MinOutRPC format=datetime19.

					from linkedrpc r
					inner join alldata l on l.linked_accountnumber=r.accountnumber and r.activitytimestamp<l.startdate
					group by 1,2,3) r on l.accountnumber=r.accountnumber and l.linked_accountnumber=r.linked_accountnumber and l.datatype=r.datatype

	left join 
		(select l.accountnumber
					,l.datatype
					,l.personid
					,max(case when i.InsolventDate<l.startdate then 1 else 0 end) as insolvent
		from Insolvent_HP i
		inner join alldata l on i.personid=l.personid
		group by 1,2,3) i on l.accountnumber=i.accountnumber and l.datatype=i.datatype

	left join 
		(select l.accountnumber
					,l.datatype
					,l.linked_accountnumber
					,max(case when missing(p.debt_code)=0 then 1 end) as linked_PrevSelected
					,max(case when missing(p.debt_code)=0 then dt_tx_date end) as linked_LitSelectionDate
		from previousselectionstemp p 
		inner join alldata l on l.linked_accountnumber=p.debt_code and p.dt_tx_date<l.startdate
		group by 1,2,3) lit on l.accountnumber=lit.accountnumber and lit.linked_accountnumber=l.linked_accountnumber and lit.datatype=l.datatype

	left join
		(select l.accountnumber
					,l.datatype
					,l.linked_accountnumber
					,max(LitigationActionDate) as linked_LitActivityDate
					,max(LitigationClaimDate) as linked_LitClaimDate
		from LitigationHistory lh
		inner join alldata l on l.linked_accountnumber=lh.debt_code and lh.LitigationActionDate<l.startdate
		group by 1,2,3) litact on l.accountnumber=litact.accountnumber and litact.linked_accountnumber=l.linked_accountnumber and litact.datatype=l.datatype

	left join
		(select distinct debtcode
		from stoplist) stop on l.linked_accountnumber=stop.debtcode

	left join 
		(select distinct l.accountnumber
					,l.datatype
					,case when missing(p.debt_code)=0 then 1 end as PrevSelected
					,case when missing(p.debt_code)=0 then dt_tx_date end as LitSelectionDate
		from previousselectionstemp p 
		inner join alldata l on l.accountnumber=p.debt_code and p.dt_tx_date<l.startdate) lit2 on l.accountnumber=lit2.accountnumber and lit2.datatype=l.datatype
		
	left join 
		(select distinct l.accountnumber
					,l.datatype
					,max(q.Fraud_Date) as Fraud_Date
		from fraud q
		inner join alldata l on l.accountnumber=q.debt_code and q.Fraud_Date<l.startdate
		group by 1,2) f on l.accountnumber=f.accountnumber and f.datatype=l.datatype
		
	left join
		(select distinct l.accountnumber
					,l.datatype
					,max(i.date) as maxdate
					,i.disposableincome
		from disposableincome i
		inner join alldata l on l.accountnumber=i.debt_code and i.date<l.startdate
		group by 1,2
		having max(i.date)=i.date) inc on l.accountnumber=inc.accountnumber and inc.datatype=l.datatype

	group by l.accountnumber, l.datatype, l.linked_accountnumber
	order by l.accountnumber
	;quit;

%mend linkeddata;

%macro custcalcs; 

	%xocalcs;

	proc sort data=linkeddata out=linkeddata2 nodupkey;
	by accountnumber datatype linked_accountnumber;
	run;

	data customerchars2_temp;
	set linkeddata2;
	by accountnumber datatype;

	linked_totalpay=sum(linked_internalpay,linked_externalpay,linked_directpay);
	linked_lastpaydate=max(linked_internalpaydate,linked_externalpaydate,linked_directpaydate);
	linked_minpaydate=min(linked_mininternalpaydate,linked_minexternalpaydate,linked_mindirectpaydate);
	linked_countpay=sum(linked_countinternalpay,linked_countexternalpay,linked_countdirectpay);

	linked_totalpayL1M=sum(linked_internalpayL1M,linked_externalpayL1M,linked_directpayL1M);
	linked_totalpayL3M=sum(linked_internalpayL3M,linked_externalpayL3M,linked_directpayL3M);
	linked_totalpayL6M=sum(linked_internalpayL6M,linked_externalpayL6M,linked_directpayL6M);
	linked_totalpayL12M=sum(linked_internalpayL12M,linked_externalpayL12M,linked_directpayL12M);

	linked_numpayments=sum(linked_countinternalpay,linked_countexternalpay,linked_countdirectpay);

	length CategoryL3Accs $100;
	length linked_PhoneNums $10000;
	length linked_PostcodeCombine $10000;

	length TraceTypeNew $20;

	if first.datatype then do;

		%initiatevars(ALL);
		%initiatevars(FS);
		%initiatevars(TEL);
		%initiatevars(MO);
		%initiatevars(SS);
		%initiatecontact;

		AccountTypeFlag='0000';
		AccountTypePaidFlag='0000';

		linked_id=0;
		CategoryL3Accs='';

		linked_PhoneNums='';
		linked_PostcodeCombine='';

		PlanVal=0;
		AmntPaidPlan=0;

		CustStoplist=0;

	end;

	retain _all_;

	array phone{6} linked_homephone--linked_phone6;

	if missing(linked_accountnumber)=0 then do;
		
		%calculations(ALL);
		%contactcalculations;

		do i=1 to 6;
			if missing(phone{i})=0 and indexw(compress(linked_PhoneNums),compress(phone{i}),'#')=0 then linked_PhoneNums=catx('#',linked_PhoneNums,compress(phone{i}));
		end;
		if missing(linked_Postcode)=0 and indexw(compress(linked_PostcodeCombine),compress(linked_Postcode),'#')=0 then linked_PostcodeCombine=catx('#',linked_PostcodeCombine,compress(linked_Postcode));
		
		if linked_sector='Financial Services' then do;

			%calculations(FS);

			if linked_statuscode^=:'918' and linked_statuscode ne '910' then do;
				substr(AccountTypeFlag,1,1)='1';
				if linked_totalpay>0 then substr(AccountTypePaidFlag,1,1)='1';
			end;

		end;
		else if linked_sector='Communications' then do;

			%calculations(TEL);

			if linked_statuscode^=:'918' and linked_statuscode ne '910' then do;
				substr(AccountTypeFlag,2,1)='1';
				if linked_totalpay>0 then substr(AccountTypePaidFlag,2,1)='1';
			end;

		end;
		else if linked_sector='Mail Order' then do;

			%calculations(MO);

			if linked_statuscode^=:'918' and linked_statuscode ne '910' then do;
				substr(AccountTypeFlag,3,1)='1';
				if linked_totalpay>0 then substr(AccountTypePaidFlag,3,1)='1';
			end;

		end;
		else if missing(linked_sector)=0 then do;

			if linked_statuscode^=:'918' and linked_statuscode ne '910' then do;
				substr(AccountTypeFlag,4,1)='1';
				if linked_totalpay>0 then substr(AccountTypePaidFlag,4,1)='1';
			end;

		end;

		if sector=linked_sector then do;
			
			%calculations(SS);

		end;

		linked_id+1;
		if linked_id<=3 then do;
			if substr(linked_statuscode,1,1) in ('2','3','5','6','8') and linked_lastpaydate>intnx('dtday',startdate,-35,'s') then CategoryL3Accs=catx('/',CategoryL3Accs,'Paying');
			else if linked_statuscode='901' then CategoryL3Accs=catx('/',CategoryL3Accs,'Settled');
			else if intnx('dtday',startdate,-730,'s')<linked_lastpaydate<=intnx('dtday',startdate,-35,'s') then CategoryL3Accs=catx('/',CategoryL3Accs,'Paid');
			else if missing(linked_accountnumber)=0 and linked_totalpay<=0 then CategoryL3Accs=catx('/',CategoryL3Accs,'Non-paid');
		end;

		CustStoplist+linked_stoplist;

	end;

	if last.datatype then do;

		%summary(ALL);
		%summary(FS);
		%summary(TEL);
		%summary(MO);
		%summary(SS);
		%contactsummary;

		if count(CategoryL3Accs,'/')=0 then CategoryL3Accs=catx('/',CategoryL3Accs,'na','na');
		if count(CategoryL3Accs,'/')=1 then CategoryL3Accs=catx('/',CategoryL3Accs,'na');

		if missing(linked_PostcodeCombine)=0 then NumExistingAddress=count(linked_PostcodeCombine,'#')+1;
		else NumExistingAddress=0;
		PurchasedAddressNew=0;
		if missing(postcode)=0 and indexw(compress(linked_PostcodeCombine),compress(postcode),'#')=0 then PurchasedAddressNew=1;

		if missing(linked_PhoneNums)=0 then NumExistingPhones=count(linked_PhoneNums,'#')+1;
		else NumExistingPhones=0;
		array purchphones{2} homephone--businessphone; /*This should include all 3 phone number fields*/
		PurchasedPhones=0;
		PurchasedPhoneNew=0;
		do i=1 to 2;
			if missing(purchphones{i})=0 then PurchasedPhones+1;
			if missing(purchphones{i})=0 and indexw(compress(linked_PhoneNums),compress(purchphones{i}),'#')=0 then PurchasedPhoneNew+1;
		end;

		output;

	end;

	drop linked: i;

	run;

	PROC SQL;
	CREATE TABLE WORK.customerchars2_temp2 AS 
	SELECT DISTINCT t1.accountnumber, 
				  t1.datatype, 
				  t1.PostCode, 
				  t1.TraceTypeNew, 
				  t2.linked_postcode, 
				  max(t2.linked_startdate) as linked_startdate format=datetime19.
				  ,count(*) as count
				  ,sum(case when linked_statuscode like '1%' then 1 end) as numnegs
	FROM WORK.customerchars2_temp t1
	   LEFT JOIN WORK.LINKEDDATA2 t2 ON (t1.accountnumber = t2.accountnumber) 
			AND (t1.datatype = t2.datatype)
	WHERE t1.TraceTypeNew = 'NA' AND compress(t1.PostCode) NOT = compress(t2.linked_postcode)
	group by 1,2,3,4,5
	order by t1.accountnumber
					,t1.datatype
					,calculated numnegs
					,calculated count desc
	;QUIT;

	data customerchars2_temp3;
	set WORK.customerchars2_temp2;
	by accountnumber datatype;
	if first.datatype;
	run;

	proc sql;
	create table customerchars2 as
	select c.*
				,case when c.tracetypenew='NA' then c3.linked_postcode
					else c.postcode
					end as tracedpostcode
	from customerchars2_temp c
	left join customerchars2_temp3 c3 on c.accountnumber=c3.accountnumber and c.datatype=c3.datatype
	;quit;

%mend custcalcs;

%benchmark;
%linkedaccs;
%data;
%linkeddata;
%custcalcs;