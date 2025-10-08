/* Unable to determine code to assign library PAYING on SASApp_Modelling */

%LET SYSLAST=WORK.ELIGIBLE_RENAME;
%LET _CLIENTTASKLABEL='Program';
%LET _CLIENTPROCESSFLOWNAME='Combine';
%LET _CLIENTPROJECTPATH='\\client_name2.local\shares\Decision Science\Pricing\Phil Young\PRICING\Paying\Paying model\Data build\data build.egp';
%LET _CLIENTPROJECTPATHHOST='LWZW059';
%LET _CLIENTPROJECTNAME='data build.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

GOPTIONS ACCESSIBLE;
proc sql;
create table paying.payingpopulation (drop=debtcode dataset) as 
select t1.*
			,t2.*
from eligible_rename t1
left join paying.external_drop t2 on t1.int_debtcode=t2.DEBTCODE
;quit;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

%LET SYSLAST=PAYING.PAYINGPOPULATION;
%LET _CLIENTTASKLABEL='Remove Missings';
%LET _CLIENTPROCESSFLOWNAME='Combine';
%LET _CLIENTPROJECTPATH='\\client_name2.local\shares\Decision Science\Pricing\Phil Young\PRICING\Paying\Paying model\Data build\data build.egp';
%LET _CLIENTPROJECTPATHHOST='LWZW059';
%LET _CLIENTPROJECTNAME='data build.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

GOPTIONS ACCESSIBLE;
options nosymbolgen;

%Macro Missing(Missing);
PROC CONTENTS NOPRINT DATA=PAYING.PAYINGPOPULATION OUT=WORK.DATASETCONTENTS;
RUN;

PROC SQL;
   CREATE TABLE WORK.NumericVar AS 
   SELECT t1.NAME
   		  ,monotonic() AS Count
      FROM WORK.DATASETCONTENTS t1
      WHERE t1.TYPE = 1 AND t1.FORMAT NOT IN 
           (
           'DATETIME',
           'DDMMYY'
           )
		   /*and t1.name="MnthsSinceLastPayment"*/
			and upper(t1.name) not in ("INT_DEBTCODE");
QUIT;

PROC SQL NOPRINT;
	SELECT MAX(COUNT) INTO: Max
	FROM WORK.NumericVar;
QUIT;

	%DO i=1 %TO &Max.;
	PROC SQL NOPRINT;
		SELECT NAME INTO:VAR&i.
		FROM WORK.NumericVar
		WHERE Count=&i.
	;
	QUIT;
	%END;

DATA WORK.DUMMY_DATASET2;
	SET PAYING.PAYINGPOPULATION;
	%DO j=1 %TO &Max.;
	IF &&Var&j.=-99999 THEN &&Var&j.=.;
	%END;
RUN;

PROC SQL;
   CREATE TABLE WORK.CountVariables AS 
   SELECT Count(t1.INT_DEBTCODE) AS NumberOfRecords
          %DO j=1 %TO &Max.;
		  ,Count(t1.&&Var&j.)/Count(t1.INT_DEBTCODE) AS &&Var&j.
		  %End;
      FROM WORK.DUMMY_DATASET2 t1;
QUIT;

PROC Transpose data=countVariables out=trn_CountVariables;
	Var %DO j=1 %TO &Max.; &&Var&j. %End; ;
Quit;

PROC SQL;
	CREATE TABLE WORK.MISSINGVARIABLES AS
	SELECT _NAME_ AS Normalised_Variables,
		   COL1 AS PercentagePopulated,
		   MONOTONIC() AS Count
	FROM WORK.trn_CountVariables
	WHERE COL1<&Missing.
;Quit;

proc print data=MISSINGVARIABLES noobs;
run;

PROC SQL NOPRINT;
	SELECT MAX(COUNT) INTO: Max2
	FROM WORK.MISSINGVARIABLES;
QUIT;

%DO i=1 %TO &Max2.;
	PROC SQL NOPRINT;
		SELECT Normalised_Variables INTO:MVAR&i.
		FROM WORK.MISSINGVARIABLES
		WHERE Count=&i.
	;
	QUIT;
%END;

Data Work.MISSING_DATASET;
	SET PAYING.PAYINGPOPULATION;
	DROP %DO j=1 %TO &Max2.; &&MVar&j. %End; ;
Run;
%MEND;

%missing(0.15);

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

%LET SYSLAST=WORK.MISSING_DATASET;
%LET _CLIENTTASKLABEL='Correlation';
%LET _CLIENTPROCESSFLOWNAME='Combine';
%LET _CLIENTPROJECTPATH='\\client_name2.local\shares\Decision Science\Pricing\Phil Young\PRICING\Paying\Paying model\Data build\data build.egp';
%LET _CLIENTPROJECTPATHHOST='LWZW059';
%LET _CLIENTPROJECTNAME='data build.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

GOPTIONS ACCESSIBLE;
%Macro Correlation(Corr);
proc corr noprint data=work.MISSING_DATASET out=work.correlation(where=(_TYPE_ = 'CORR'));
	var _numeric_;
run;

proc sort;
	by _NAME_;
Quit;

Data Work.Numbered_Correlation;
	Rename _NAME_ = Normalised_Variable;
	set Work.Correlation(drop=_TYPE_);
	N=_N_;
Run;

Proc SQL noprint;
	select max(N) into: Max
	From Work.Numbered_Correlation;
Quit;

%DO i=1 %TO &Max.;
	PROC SQL NOPRINT;
		SELECT Normalised_Variable INTO:VAR&i.
		FROM WORK.Numbered_Correlation
		WHERE N=&i.
	;
	QUIT;
%END;

Proc transpose data=work.Numbered_Correlation(drop=N) out=work.trn_correlation;
	By Normalised_Variable;
	Var %DO j=1 %TO &Max.;
		  &&Var&j.
		  %End; ;
Run;Quit;

PROC SQL;
   CREATE TABLE WORK.CorrelatedVariables AS 
   SELECT t1.Normalised_Variable, 
          t1._NAME_, 
          t1.COL1 as Correlation
      FROM WORK.TRN_CORRELATION t1
      WHERE abs(t1.COL1) >= &Corr. AND t1.COL1 NOT = 1
	Order by t1.Normalised_Variable;
QUIT;

Data WORK.CorrelatedVariables2;
	Set WORK.CorrelatedVariables;
	By Normalised_Variable;
	If first.Normalised_Variable then N=0;
	N+1;
Run;

PROC SQL;
   CREATE TABLE WORK.CountCorrelatedVariables AS 
   SELECT t1.Normalised_Variable, 
            (COUNT(t1.Normalised_Variable)) AS COUNT
      FROM WORK.CorrelatedVariables t1
      GROUP BY t1.Normalised_Variable
      ORDER BY COUNT desc;
QUIT;

Proc SQL;	
	Create table work.Numbered_CorrelatedVariables as
	Select *, monotonic() as N
	From WORK.CountCorrelatedVariables;
Quit;

proc sql noprint;	
	select count(Normalised_Variable) into: Remaining
	From work.Numbered_CorrelatedVariables
;
quit;

Data Work.RemoveCorrelation;
	Set Work.CorrelatedVariables;
run;
%Do %until (&Remaining.=0);

PROC SQL noprint;
	select min(N) into:Min
	From work.Numbered_CorrelatedVariables;
	Select count into:count
	from work.Numbered_CorrelatedVariables
	where N=&min.;
	Select Normalised_Variable into:Var
	from work.Numbered_CorrelatedVariables
	where N=&min.;
Quit;

%Do i=1 %to &count.;
	proc sql noprint;
		select _NAME_ into: Corr&i.
		from CorrelatedVariables2
		where N=&i.
			and Normalised_Variable="&Var.";
	Quit;
%End;

Data Work.RemoveCorrelation;
	Set Work.RemoveCorrelation;
	if Normalised_Variable not in("%cmpres(&Corr1.)" %Do i=2 %to &count.; ,"%cmpres(&&Corr&i.)" %end;) ;
Run;

Data Work.Numbered_CorrelatedVariables;
	Set Work.Numbered_CorrelatedVariables;
	if Normalised_Variable not in("%cmpres(&Corr1.)" %Do i=2 %to &count.; ,"%cmpres(&&Corr&i.)" %end;) ;
	if Normalised_Variable ne "%cmpres(&var.)";
Run;

proc sql noprint;	
	select count(Normalised_Variable) into: Remaining
	From work.Numbered_CorrelatedVariables
;
quit;
%put Correlated Variables Remaining are &Remaining.;
%End;
PROC SQL;
   CREATE TABLE WORK.NONCorrelatedVariables AS 
   SELECT distinct t1.Normalised_Variable
      FROM WORK.TRN_CORRELATION t1
      WHERE Normalised_Variable not in(select Normalised_Variable from work.correlatedvariables)
	Order by t1.Normalised_Variable;
QUIT;

PROC SQL;
   CREATE TABLE WORK.KeptCorrelation AS 
   SELECT distinct t1.Normalised_Variable
      FROM WORK.RemoveCorrelation t1
	Order by t1.Normalised_Variable;
QUIT;

Data Work.RemainingVariables;
	Set WORK.NONCorrelatedVariables
		WORK.KeptCorrelation;
	N+1;
Run;
proc sql noprint;
	select max(N) into:Max2
	from work.remainingvariables;
quit;

%DO i=1 %TO &Max2.;
	PROC SQL NOPRINT;
		SELECT Normalised_Variable INTO:CVAR&i.
		FROM WORK.RemainingVariables
		WHERE N=&i.
	;
	QUIT;
%END;

data _null_;
set sashelp.vcolumn (keep=libname memname name type) end=last;
where libname='WORK' and memname='MISSING_DATASET' and type='char';
call symputx(cats('char',_n_),name);
if last then call symputx('numchar',_n_);
run;

PROC SQL;
   CREATE TABLE WORK.NonCorr_DataSet AS 
   SELECT t1.INT_DebtCode
          %DO j=1 %TO &Max2.;
			  ,t1.&&CVar&j.
		  %End;
		  %do k=1 %to &numchar;
		  	,&&char&k
		  %end;
      FROM WORK.MISSING_DATASET t1;
QUIT;
%Mend;

%Correlation(0.9);


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

%LET SYSLAST=WORK.NONCORR_DATASET;
%LET _CLIENTTASKLABEL='Program (2)';
%LET _CLIENTPROCESSFLOWNAME='Combine';
%LET _CLIENTPROJECTPATH='\\client_name2.local\shares\Decision Science\Pricing\Phil Young\PRICING\Paying\Paying model\Data build\data build.egp';
%LET _CLIENTPROJECTPATHHOST='LWZW059';
%LET _CLIENTPROJECTNAME='data build.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

GOPTIONS ACCESSIBLE;
data paying.population_cleansed;
set NONCORR_DATASET;

array num{*} _numeric_;
array char{*} _character_;

do i=1 to dim(num);
	if missing(num{i}) then num{i}=-99999;
end;
do i=1 to dim(char);
	if missing(char{i}) then char{i}='?????';
end;

run;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;