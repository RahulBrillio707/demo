
%macro searchvars(type);
	
	NumSearches_&type=0;
	NumSearchesL12M_&type=0;
	NumSearchesL6M_&type=0;
	NumSearchesL3M_&type=0;

	MostRecentSearch_&type=.;	

	length hurn_retain_&type $20000;
	hurn_retain_&type='';
	NumAddsSearched_&type=0;

%mend;



%macro searches(type);

	if	applicationdate<sampledate then NumSearches_&type+1;
	if	intnx('month',sampledate,-12,'s')<applicationdate<sampledate then NumSearchesL12M_&type+1;
	if	intnx('month',sampledate,-6,'s')<applicationdate<sampledate then NumSearchesL6M_&type+1;
	if	intnx('month',sampledate,-3,'s')<applicationdate<sampledate then NumSearchesL3M_&type+1;

	if	intnx('month',sampledate,-12,'s')<applicationdate<sampledate then NumSearchesL12M_&type+1;
	if	intnx('month',sampledate,-6,'s')<applicationdate<sampledate then NumSearchesL6M_&type+1;
	if	intnx('month',sampledate,-3,'s')<applicationdate<sampledate then NumSearchesL3M_&type+1;

	MostRecentSearch_&type=max(MostRecentSearch_&type,applicationdate);

	if indexw(compress(hurn_retain_&type),compress(put(locationid,20.)),'#')=0 then do;
		NumAddsSearched_&type+1;
		hurn_retain_&type=catx('#',hurn_retain_&type,locationid);
	end;

%mend;



%macro searchcalcs(type);

	if missing(MostRecentSearch_&type)=0 then AgeRecentSearch_&type = intck('month',MostRecentSearch_&type,sampledate);								else AgeRecentSearch_&type = -999999;

	drop hurn_retain_&type MostRecentSearch_&type;

%mend;


%macro calcs(months);

	if first.sampledate then do;
		MaritalStatus_F_&months=.;
		NumDependants_F_&months=.;
		AccommodationType_F_&months=.;
		EmploymentType_F_&months=.;
		Salary_F_&months=.;
		TimeInEmployment_F_&months=.;
		TimeAtAddress_F_&months=.;
		TimeBank_F_&months=.;

		MaritalStatus_L_&months=.;
		NumDependants_L_&months=.;
		AccommodationType_L_&months=.;
		EmploymentType_L_&months=.;
		Salary_L_&months=.;
		TimeInEmployment_L_&months=.;
		TimeAtAddress_L_&months=.;
		TimeBank_L_&months=.;
	end;

	if intnx('months',sampledate,-1*(&months))<=applicationdate<=sampledate then do;
		if missing(MaritalStatusID)=0 then MaritalStatus_F_&months=MaritalStatusID;
		if missing(MaritalStatus_L_&months)=1 then MaritalStatus_L_&months=MaritalStatusID;

		if missing(Dependents)=0 then NumDependants_F_&months=Dependents;
		if missing(NumDependants_L_&months)=1 then NumDependants_L_&months=Dependents;

		if missing(Accommodationtypeid)=0 then AccommodationType_F_&months=Accommodationtypeid;
		if missing(AccommodationType_L_&months)=1 then AccommodationType_L_&months=Accommodationtypeid;

		if missing(EmploymentStatusid)=0 then EmploymentStatus_F_&months=EmploymentStatusid;
		if missing(EmploymentStatus_L_&months)=1 then EmploymentStatus_L_&months=EmploymentStatusid;

		if missing(EmploymentTypeid)=0 then EmploymentType_F_&months=EmploymentTypeid;
		if missing(EmploymentType_L_&months)=1 then EmploymentType_L_&months=EmploymentTypeid;

		if missing(grossincome)=0 then Salary_F_&months=grossincome;
		if missing(Salary_L_&months)=1 then Salary_L_&months=grossincome;

		if timeemployment not in ('NA','NG') and missing(timeemployment)=0 then TimeInEmployment_F_&months=input(timeemployment,8.);
		if timeemployment not in ('NA','NG') and missing(TimeInEmployment_L_&months)=1 then TimeInEmployment_L_&months=input(timeemployment,8.);

		if TimeAddress not in ('NA','NG') and missing(TimeAddress) = 0 then TimeAtAddress_F_&months = input(TimeAddress,8.);
		if TimeAddress not in ('NA','NG') and missing(TimeAtAddress_L_&months) = 1 then TimeAtAddress_L_&months = input(TimeAddress,8.);

		if timewithbank not in ('NA','NG') and missing(timewithbank)=0 then TimeBank__&months=input(timewithbank,8.);
		if timewithbank not in ('NA','NG') and missing(TimeBank_L_&months)=1 then TimeBank_L_&months=input(timewithbank,8.);

	end;
	else do;
		MaritalStatus_F_&months=MaritalStatus_F_&months;
		NumDependants_F_&months=NumDependants_F_&months;
		AccommodationType_F_&months=AccommodationType_F_&months;
		EmploymentType_F_&months=EmploymentType_F_&months;
		Salary_F_&months=Salary_F_&months;
		TimeInEmployment_F_&months=TimeInEmployment_F_&months;
		TimeAtAddress_F_&months=TimeAtAddress_F_&months;
		TimeBank_F_&months=TimeBank_F_&months;
	end;


%mend;

%macro ecapsdesc(months);

PROC SQL;
	CREATE TABLE work.description_&months AS 
		SELECT t1.AccountNumber,
			t1.SampleDate,
			t1.NumDependants_F_&months,
			t1.Salary_F_&months,
			t1.TimeInEmployment_F_&months,
			t1.TimeAtAddress_F_&months,
			t1.TimeBank_F_&months,
			t1.NumDependants_L_&months,
			t1.Salary_L_&months,
			t1.TimeInEmployment_L_&months,
			t1.TimeAtAddress_L_&months,
			t1.TimeBank_L_&months,
			t2.Description AS AccommodationTypeDesc_L_&months, 
			t3.Description AS EmploymentStatusDesc_L_&months, 
			t4.Description AS EmploymentTypeDesc_L_&months, 
			t5.Description AS MaritalStatusDesc_L_&months,
			t6.Description AS AccommodationTypeDesc_F_&months, 
			t7.Description AS EmploymentStatusDesc_F_&months, 
			t8.Description AS EmploymentTypeDesc_F_&months, 
			t9.Description AS MaritalStatusDesc_F_&months
		FROM work.&out_data t1
			left join BDATA.AccommodationType t2 on t1.AccommodationType_L_&months = t2.AccommodationTypeID 
			left join BDATA.EmploymentStatus t3 on t1.EmploymentStatus_L_&months = t3.EmploymentStatusID 
			left join BDATA.EmploymentType t4 on t1.EmploymentType_L_&months = t4.EmploymentTypeID 
			left join BDATA.MaritalStatus t5 on t1.MaritalStatus_L_&months = t5.MaritalStatusID
			left join BDATA.AccommodationType t6 on t1.AccommodationType_F_&months = t6.AccommodationTypeID 
			left join BDATA.EmploymentStatus t7 on t1.EmploymentStatus_F_&months = t7.EmploymentStatusID 
			left join BDATA.EmploymentType t8 on t1.EmploymentType_F_&months = t8.EmploymentTypeID 
			left join BDATA.MaritalStatus t9 on t1.MaritalStatus_F_&months = t9.MaritalStatusID
		Order by t1.AccountNumber, t1.SampleDate;
QUIT;

%mend;

%macro AppType(months,AppType);

	if first.sampledate then do;
		MaritalStatus_F_&months._&AppType=.;
		NumDependants_F_&months._&AppType=.;
		AccommodationType_F_&months._&AppType=.;
		EmploymentType_F_&months._&AppType=.;
		Salary_F_&months._&AppType=.;
		TimeInEmployment_F_&months._&AppType=.;
		TimeAtAddress_F_&months._&AppType=.;
		TimeBank_F_&months._&AppType=.;

		MaritalStatus_L_&months._&AppType=.;
		NumDependants_L_&months._&AppType=.;
		AccommodationType_L_&months._&AppType=.;
		EmploymentType_L_&months._&AppType=.;
		Salary_L_&months._&AppType=.;
		TimeInEmployment_L_&months._&AppType=.;
		TimeAtAddress_L_&months._&AppType=.;
		TimeBank_L_&months._&AppType=.;
	end;

	if AppCode = "&AppType" and intnx('months',sampledate,-1*(&months))<=applicationdate<=sampledate then do;
			if missing(MaritalStatusID)=0 then MaritalStatus_F_&months._&AppType=MaritalStatusID;
			if missing(MaritalStatus_L_&months._&AppType)=1 then MaritalStatus_L_&months._&AppType=MaritalStatusID;

			if missing(Dependents)=0  then NumDependants_F_&months._&AppType=Dependents;
			if missing(NumDependants_L_&months._&AppType)=1 then NumDependants_L_&months._&AppType=Dependents;

			if missing(Accommodationtypeid)=0  then AccommodationType_F_&months._&AppType=Accommodationtypeid;
			if missing(AccommodationType_L_&months._&AppType)=1  then AccommodationType_L_&months._&AppType=Accommodationtypeid;

			if missing(EmploymentStatusid)=0  then EmploymentStatus_F_&months._&AppType=EmploymentStatusid;
			if missing(EmploymentStatus_L_&months._&AppType)=1  then EmploymentStatus_L_&months._&AppType=EmploymentStatusid;

			if missing(EmploymentTypeid)=0  then EmploymentType_F_&months._&AppType=EmploymentTypeid;
			if missing(EmploymentType_L_&months._&AppType)=1  then EmploymentType_L_&months._&AppType=EmploymentTypeid;

			if missing(grossincome)=0  then Salary_F_&months._&AppType=grossincome;
			if missing(Salary_L_&months._&AppType)=1  then Salary_L_&months._&AppType=grossincome;

			if timeemployment not in ('NA','NG') and missing(timeemployment)=0  then TimeInEmployment_F_&months._&AppType=input(timeemployment,8.);
			if timeemployment not in ('NA','NG') and missing(TimeInEmployment_L_&months._&AppType)=1  then TimeInEmployment_L_&months._&AppType=input(timeemployment,8.);

			if TimeAddress not in ('NA','NG') and missing(TimeAddress) = 0  then TimeAtAddress_F_&months._&AppType = input(TimeAddress,8.);
			if TimeAddress not in ('NA','NG') and missing(TimeAtAddress_L_&months._&AppType) = 1  then TimeAtAddress_L_&months._&AppType = input(TimeAddress,8.);

			if timewithbank not in ('NA','NG') and missing(timewithbank)=0  then TimeBank_F_&months._&AppType=input(timewithbank,8.);
			if timewithbank not in ('NA','NG') and missing(TimeBank_L_&months._&AppType)=1  then TimeBank_L_&months._&AppType=input(timewithbank,8.);
	end;
	else do;
		MaritalStatus_F_&months._&AppType=MaritalStatus_F_&months._&AppType;
		NumDependants_F_&months._&AppType=NumDependants_F_&months._&AppType;
		AccommodationType_F_&months._&AppType=AccommodationType_F_&months._&AppType;
		EmploymentType_F_&months._&AppType=EmploymentType_F_&months._&AppType;
		Salary_F_&months._&AppType=Salary_F_&months._&AppType;
		TimeInEmployment_F_&months._&AppType=TimeInEmployment_F_&months._&AppType;
		TimeAtAddress_F_&months._&AppType=TimeAtAddress_F_&months._&AppType;
		TimeBank_F_&months._&AppType=TimeBank_F_&months._&AppType;

		MaritalStatus_L_&months._&AppType=MaritalStatus_L_&months._&AppType;
		NumDependants_L_&months._&AppType=NumDependants_L_&months._&AppType;
		AccommodationType_L_&months._&AppType=AccommodationType_L_&months._&AppType;
		EmploymentType_L_&months._&AppType=EmploymentType_L_&months._&AppType;
		Salary_L_&months._&AppType=Salary_L_&months._&AppType;
		TimeInEmployment_L_&months._&AppType=TimeInEmployment_L_&months._&AppType;
		TimeAtAddress_L_&months._&AppType=TimeAtAddress_L_&months._&AppType;
		TimeBank_L_&months._&AppType=TimeBank_L_&months._&AppType;
	end;


%mend;

%macro calcsAppType(AppType);

%AppType(3,&AppType);
%AppType(6,&AppType);
%AppType(9,&AppType);
%AppType(12,&AppType);
%AppType(15,&AppType);
%AppType(18,&AppType);
%AppType(24,&AppType);

%mend;

%macro AppTypeDesc(months,AppType);

PROC SQL;
	CREATE TABLE work.description_&months._&AppType AS 
		SELECT t1.AccountNumber,
			t1.SampleDate,
			t1.NumDependants_F_&months._&AppType,
			t1.Salary_F_&months._&AppType,
			t1.TimeInEmployment_F_&months._&AppType,
			t1.TimeAtAddress_F_&months._&AppType,
			t1.TimeBank_F_&months._&AppType,
			t1.NumDependants_L_&months._&AppType,
			t1.Salary_L_&months._&AppType,
			t1.TimeInEmployment_L_&months._&AppType,
			t1.TimeAtAddress_L_&months._&AppType,
			t1.TimeBank_L_&months._&AppType,
			t2.Description AS AccommodationTypeDesc_L_&months._&AppType, 
			t3.Description AS EmploymentStatusDesc_L_&months._&AppType, 
			t4.Description AS EmploymentTypeDesc_L_&months._&AppType, 
			t5.Description AS MaritalStatusDesc_L_&months._&AppType,
			t6.Description AS AccommodationTypeDesc_F_&months._&AppType, 
			t7.Description AS EmploymentStatusDesc_F_&months._&AppType, 
			t8.Description AS EmploymentTypeDesc_F_&months._&AppType, 
			t9.Description AS MaritalStatusDesc_F_&months._&AppType
		FROM work.&out_data t1
			left join BDATA.AccommodationType t2 on t1.AccommodationType_L_&months._&AppType = t2.AccommodationTypeID 
			left join BDATA.EmploymentStatus t3 on t1.EmploymentStatus_L_&months._&AppType = t3.EmploymentStatusID 
			left join BDATA.EmploymentType t4 on t1.EmploymentType_L_&months._&AppType = t4.EmploymentTypeID 
			left join BDATA.MaritalStatus t5 on t1.MaritalStatus_L_&months._&AppType = t5.MaritalStatusID
			left join BDATA.AccommodationType t6 on t1.AccommodationType_F_&months._&AppType = t6.AccommodationTypeID 
			left join BDATA.EmploymentStatus t7 on t1.EmploymentStatus_F_&months._&AppType = t7.EmploymentStatusID 
			left join BDATA.EmploymentType t8 on t1.EmploymentType_F_&months._&AppType = t8.EmploymentTypeID 
			left join BDATA.MaritalStatus t9 on t1.MaritalStatus_F_&months._&AppType = t9.MaritalStatusID
		Order by t1.AccountNumber, t1.SampleDate;
QUIT;

%mend;
%macro ecapsdescAppType(AppType);

%AppTypeDesc(3,&AppType);
%AppTypeDesc(6,&AppType);
%AppTypeDesc(9,&AppType);
%AppTypeDesc(12,&AppType);
%AppTypeDesc(15,&AppType);
%AppTypeDesc(18,&AppType);
%AppTypeDesc(24,&AppType);

%mend;

%macro dropApp(months,AppType);

drop 	AccommodationType_F_&months._&AppType		EmploymentStatus_F_&months._&AppType 
		EmploymentType_F_&months._&AppType		MaritalStatus_F_&months._&AppType
		AccommodationType_L_&months._&AppType		EmploymentStatus_L_&months._&AppType 
		EmploymentType_L_&months._&AppType		MaritalStatus_L_&months._&AppType;

%mend;

%macro dropAppType(AppType);

%dropApp(3,&AppType);
%dropApp(6,&AppType);
%dropApp(9,&AppType);
%dropApp(12,&AppType);
%dropApp(15,&AppType);
%dropApp(18,&AppType);
%dropApp(24,&AppType);

%mend;

%macro drop(months);

drop 	AccommodationType_F_&months		EmploymentStatus_F_&months
		EmploymentType_F_&months		MaritalStatus_F_&months
		AccommodationType_L_&months		EmploymentStatus_L_&months
		EmploymentType_L_&months		MaritalStatus_L_&months;


%mend;


