* STEP 1:																		  ;
* REDIRECT SCREEN OUTPUT TO LOG FILE.  WHAT FOLLOWS CONTROLS FILENAME AND LOCATION;

data _null_;
  length rundate $12;
  rundateT = datetime();
  rundate=tranwrd(put(datepart(rundateT), ddmmyy6.),'/','');
  rundate = trim(rundate);
  call symput("RUNID", rundateT);
  call symput("RUNDATE", rundate);
run;

* Get the USER ID for the person running the script;
%mduextr(libref=work);
data _NULL_;
	set work.Logins_INFO;

	UserID = compress(UserID);
	UserID = tranwrd(UserId, "\", "_");
	call symput("USER_ID", UserID);
run;

%put USER ID: (%sysfunc(COMPRESS(&USER_ID.)));

* Setup the logfile name with current date + time to the minute and regression date if exists;
data _null_;
	length mydate $6;
	length myhour $2;
	length myminute $2;
	length dtstring $12;
	length logFilename $150;

	mydate = compress("&RUNDATE.");
	myhour = compress(hour(time()));
	myminute = compress(minute(time()));
	put  myhour "ddd";
	put  myminute "mmm";
	put  mydate;

	call cats(dtstring, mydate, "_", myhour,"_", myminute);
	* Print to File;
	%let USER_ID = %sysfunc(COMPRESS(&USER_ID.));
	logFilename = "log = '&logpathlocation.\DataBuild_(&USER_ID.)_&Rundate..log'";
	logFilename = logFilename;
	* Print to Screen;
	* logFilename = "logFileLoc = ";

	put logFilename;
	call symput("logFileLoc", logFilename);
run;

%put NOTE: logFileLoc = &logFileLoc.;

* Set this macro variable to the physical location of where you want the log to go to;
* Macro to check if a macro variable is set or not;
%macro isBlank(param); 
  %sysevalf(%superq(param)=,boolean);
%mend;

* As it says on the tin, setup the logfile ;
%macro setupPrintLog();
	%global logFileLoc;
	%if %isBlank(logFileLoc) eq 0 %then %do;
		FILENAME datafil &logFileLoc.;
		 DATA _NULL_;
		  rc=FDELETE('datafil');
		  SELECT (rc);
		    WHEN (0) put 'Note: LogFile has been deleted!';
		    WHEN (20006) put 'Note: LogFile does not exist, this must be a clean run';
		    OTHERWISE;
		  END;
		  STOP;
		RUN;
		FILENAME datafil CLEAR;
	%end;

	%PUT NOTE: Log File Location - &logFileLoc.;
	proc printto &logFileLoc.;
	run;
%mend;

* As the logfile is too large for console and we need to have an archive, setup the unique file name and location;
%setupPrintLog;