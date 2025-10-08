%macro public(dataset,debtcode,sampledate);

	libname bdata meta library='bureaudata' repname='foundation';
	libname bctl meta library='bureaudata.control' repname='foundation';
	
	%inc '\\client_name2.local\shares\SASWork\DATA\Variable Build\Macros\Bureau-Experian\PublicCalculations.sas';
	%PublicCalculations;

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

	PROC SQL;
	   CREATE TABLE publicmapping AS 
	   SELECT t1.PublicInformationID, 
	          t1.PersonID, 
	          t1.LocationID, 
	          t2.PublicInformationDate, 
	          t2.settleddate,
			  t2.amount,
			  t2.PublicInformationTypeID
	      FROM BDATA.PublicInformationMapping t1
	           INNER JOIN BDATA.PublicInformation t2 ON (t1.PublicInformationID = t2.PublicInformationID)
	      WHERE t1.currentind = 1;
	QUIT;

	data publicdata;
	if _n_=1 then do;
		declare hash h (dataset:'debtcode_pid');
		h.definekey('personid');
		h.definedata('sampledate','accountnumber');
		h.definedone();
	end;
	set publicmapping;
	length accountnumber $10;
	call missing(sampledate,accountnumber);
	format sampledate date9.;
	f=h.find();
	if PublicInformationDate>sampledate then delete;
	if f=0;
	drop f;
	run;

	proc sort data=publicdata;
	by accountnumber sampledate descending PublicInformationDate;
	run;

	data work.publicsummary;
	set WORK.publicdata;
	by accountnumber sampledate;
		
	if first.sampledate then do;
		%initial(All);
		/*%initial(CCJ); - Multi*/
		%initial(DRO);
		%initial(IVA);
		%initial(Bank);
		/*%initial(AO); - Not very populated*/
		%initial(SEQ);
		%initial(STDeed);
	end;

	retain _all_;

	/* All */
	if missing(PublicInformationTypeID)=0 then do;
		%publiccalcs(All);
	end;
	/* Judgement  - Multi
	if PublicInformationTypeID in (1137,1134,1135,1138,1197,1198) then do;
		%publiccalcs(CCJ);
	end;*/
	/* Debt relief order */
	if PublicInformationTypeID in (1195,1200,1201) then do;
		%publiccalcs(DRO);
	end;
	/* Voluntary arrangement */
	if PublicInformationTypeID in (1162,1166,1163,1165) then do;
		%publiccalcs(IVA);
	end;
	/* Bankruptcy */
	if PublicInformationTypeID in (1157,1182,1183) then do;
		%publiccalcs(Bank);
	end;
	/* Administration order  - Not very populated
	if PublicInformationTypeID in (1131,1149,1150,1160) then do;
		%publiccalcs(AO);
	end;*/
	/* Sequestration */
	if PublicInformationTypeID in (1152) then do;
		%publiccalcs(SEQ);
	end;
	/* Scottish Trust Deed */
	if PublicInformationTypeID in (1193) then do;
		%publiccalcs(STDeed);
	end;

	if last.sampledate then do;
		%pubsummarycalcs(All);
		/*%pubsummarycalcs(CCJ); - Multi*/
		%pubsummarycalcs(DRO);
		%pubsummarycalcs(IVA);
		%pubsummarycalcs(Bank);
		/*%pubsummarycalcs(AO); Not very populated*/
		%pubsummarycalcs(SEQ);
		%pubsummarycalcs(STDeed);
		output;
	end;

	drop PublicInformationDate PublicInformationID PublicInformationTypeID LocationID SettledDate amount 
		Val_DRO: NumSettled_DRO: ValSettled_DRO: AgeOldestSettled_DRO DiffOldestNewSettle_DRO DiffOldestNew_DRO /* DROs don't have an amount,  can't be settled, and generally have one*/
		Val_SEQ: ValSettled_SEQ: DiffOldestNew_Seq DiffOldestNewSettle_Seq /* Sequestrations don't have an amount and generally have one */
		Val_STDeed: NumSettled_STDeed: ValSettled_STDeed: AgeOldestSettled_STDeed DiffOldestNewSettle_STDeed /* Scottish trust deeds don't have an amount,  can't be settled and usually have one*/
		Val_Bank: ValSettled_Bank: DiffOldestNew_Bank DiffOldestNewSettle_Bank /* Bankruptcies don't really have values and only usually have one */
	;

	run;

	proc sql;
	drop table accountmapping,
		_sample,
		debtcode_pid,
		publicmapping,
		publicdata
	;quit;

%mend;