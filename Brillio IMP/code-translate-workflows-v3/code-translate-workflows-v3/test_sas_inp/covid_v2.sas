/* SAS Program COVID_19 
Cleveland Clinic and SAS Collaboarion

These models are only as good as their inputs. 
Input values for this type of model are very dynamic and may need to be evaluated across wide ranges and reevaluated as the epidemic progresses.  
This work is currently defaulting to values for the population studied in the Cleveland Clinic and SAS collaboration.
You need to evaluate each parameter for your population of interest.
*/

/* directory path for files: COVID_19.sas (this file), libname store */
%let homedir = /Local_Files/covid-19-sas/ccf;

/* the storage location for the MODEL_FINAL table and other output tables - when &ScenarioSource=BATCH */
libname store "&homedir.";

/* Depending on which SAS products you have and which releases you have these options will turn components of this code on/off */
%LET HAVE_SASETS = YES; /* YES implies you have SAS/ETS software, this enable the PROC MODEL methods in this code.  Without this the Data Step SIR model still runs */
%LET HAVE_V151 = NO; /* YES implies you have products verison 15.1 (latest) and switches PROC MODEL to PROC TMODEL for faster execution */

/*  User Interface Switches
- BATCH: Default mode 
- UI: Used when using SAS Visual Analytics
- BOEMSKA: Used when using the Boemska App 
*/
%bafGetDatasets; /* get all input tables */
/* TODO Alessio not yet supported    resetline; /* for error reconciliation */

%LET ScenarioSource = BOEMSKA;

%macro EasyRun(Scenario,IncubationPeriod,InitRecovered,RecoveryDays,doublingtime,Population,KnownAdmits,
                SocialDistancing,ISOChangeDate,ISOChangeEvent,ISOChangeWindow,SocialDistancingChange,
                MarketSharePercent,Admission_Rate,ICUPercent,VentPErcent,FatalityRate,
                plots=no,N_DAYS=365,DiagnosedRate=1.0,E=0,SIGMA=3,DAY_ZERO='13MAR2020'd,
                ECMO_RATE=0.03,DIAL_RATE=0.05,HOSP_LOS=7,ICU_LOS=9,VENT_LOS=10,ECMO_LOS=6,DIAL_LOS=11);

    DATA INPUTS;
        FORMAT
            Scenario                    $200.     
            IncubationPeriod            BEST12.    
            InitRecovered               BEST12.  
            RecoveryDays                BEST12.    
            doublingtime                BEST12.    
            Population                  BEST12.    
            KnownAdmits                 BEST12.    
            SocialDistancing            BEST12.    
            ISOChangeDate               $200.    
            ISOChangeEvent              $200.
            ISOChangeWindow             $50.
            SocialDistancingChange      $50.     
            MarketSharePercent          BEST12.    
            Admission_Rate              BEST12.    
            ICUPercent                  BEST12.    
            VentPErcent                 BEST12.    
            FatalityRate                BEST12.   
            plots                       $3.
            N_DAYS                      BEST12.
            DiagnosedRate               BEST12.
            E                           BEST12.
            SIGMA                       BEST12.
            DAY_ZERO                    DATE9.
            ECMO_RATE                   BEST12.
            DIAL_RATE                   BEST12.
            HOSP_LOS                    $100.
            ICU_LOS                     $100.
            VENT_LOS                    $100.
            ECMO_LOS                    $100.
            DIAL_LOS                    $100.
        ;
        LABEL
            Scenario                    =   "Scenario Name"
            IncubationPeriod            =   "Average Days between Infection and Hospitalization"
            InitRecovered               =   "Number of Recovered (Immune) Patients on Day 0"
            RecoveryDays                =   "Average Days Infectious"
            doublingtime                =   "Baseline Infection Doubling Time (No Social Distancing)"
            Population                  =   "Regional Population"
            KnownAdmits                 =   "Number of Admitted Patients in Hospital of Interest on Day 0"
            SocialDistancing            =   "Initial Social Distancing (% Reduction from Normal)"
            ISOChangeDate               =   "Dates of Change in Social Distancing"
            ISOChangeEvent              =   "Event label associated with ISOChangeDate"
            ISOChangeWindow             =   "Number of Days to rollout Social Distancing Change"
            SocialDistancingChange      =   "Social Distancing Change (% Reduction from Normal)"
            MarketSharePercent          =   "Anticipated Share (%) of Regional Hospitalized Patients"
            Admission_Rate              =   "Percentage of Infected Patients Requiring Hospitalization"
            ICUPercent                  =   "Percentage of Hospitalized Patients Requiring ICU"
            VentPErcent                 =   "Percentage of Hospitalized Patients Requiring Ventilators"
            FatalityRate                =   "Percentage of Hospitalized Patients who will Die"
            plots                       =   "Display Plots (Yes/No)"
            N_DAYS                      =   "Number of Days to Project"
            DiagnosedRate               =   "Hospitalization Rate Reduction (%) for Underdiagnosis"
            E                           =   "Number of Exposed Patients on Day 0"
            SIGMA                       =   "Days Exposed before Infected"
            DAY_ZERO                    =   "Date of the First COVID-19 Case"
            ECMO_RATE                   =   "Percentage of Hospitalized Patients Requiring ECMO"
            DIAL_RATE                   =   "Percentage of Hospitalized Patients Requiring Dialysis"
            HOSP_LOS                    =   "Hospital Length of Stay"
            ICU_LOS                     =   "ICU Length of Stay"
            VENT_LOS                    =   "Ventilator Length of Stay"
            ECMO_LOS                    =   "ECMO Length of Stay"
            DIAL_LOS                    =   "Dialysis Length of Stay"
        ;
        Scenario                    =   "&Scenario.";
        IncubationPeriod            =   &IncubationPeriod.;
        InitRecovered               =   &InitRecovered.;
        RecoveryDays                =   &RecoveryDays.;
        doublingtime                =   &doublingtime.;
        Population                  =   &Population.;
        KnownAdmits                 =   &KnownAdmits.;
        SocialDistancing            =   &SocialDistancing.;
        ISOChangeDate               =   "&ISOChangeDate.";
        ISOChangeEvent              =   "&ISOChangeEvent.";
        ISOChangeWindow             =   "&ISOChangeWindow.";
        SocialDistancingChange      =   "&SocialDistancingChange.";
        MarketSharePercent          =   &MarketSharePercent.;
        Admission_Rate              =   &Admission_Rate.;
        ICUPercent                  =   &ICUPercent.;
        VentPErcent                 =   &VentPErcent.;
        FatalityRate                =   &FatalityRate.;
        plots                       =   "&plots.";
        N_DAYS                      =   &N_DAYS.;
        DiagnosedRate               =   &DiagnosedRate.;
        E                           =   &E.;
        SIGMA                       =   &SIGMA.;
        DAY_ZERO                    =   &DAY_ZERO.;
        ECMO_RATE                   =   &ECMO_RATE.;
        DIAL_RATE                   =   &DIAL_RATE.;
        HOSP_LOS                    =   "&HOSP_LOS.";
        ICU_LOS                     =   "&ICU_LOS.";
        VENT_LOS                    =   "&VENT_LOS.";
        ECMO_LOS                    =   "&ECMO_LOS.";
        DIAL_LOS                    =   "&DIAL_LOS.";
    RUN;

    %IF &ScenarioSource = UI %THEN %DO;
        /* this session is only used for reading the SCENARIOS table in the global caslib when the UI is running the scenario */
        %LET PULLLIB=&CASSource.;
    %END;
    %ELSE %DO;
        %LET PULLLIB=store;
    %END;

    /* create an index, ScenarioIndex for this run by incrementing the max value of ScenarioIndex in SCENARIOS dataset */
        %IF %SYSFUNC(exist(&PULLLIB..scenarios)) %THEN %DO;
            PROC SQL noprint; select max(ScenarioIndex) into :ScenarioIndex_Base from &PULLLIB..scenarios where ScenarioSource="&ScenarioSource."; quit;
            /* this may be the first ScenarioIndex for the ScenarioSource - catch and set to 0 */
            %IF &ScenarioIndex_Base = . %THEN %DO; %LET ScenarioIndex_Base = 0; %END;
        %END;
        %ELSE %DO; %LET ScenarioIndex_Base = 0; %END;
        %LET ScenarioIndex = %EVAL(&ScenarioIndex_Base + 1);

    /* store all the macro variables that set up this scenario in SCENARIOS dataset */
        DATA SCENARIOS;
            set sashelp.vmacro(where=(scope='EASYRUN'));
            if name in ('SQLEXITCODE','SQLOBS','SQLOOPS','SQLRC','SQLXOBS','SQLXOPENERRS','SCENARIOINDEX_BASE','PULLLIB') then delete;
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
            STAGE='INPUT';
        RUN;
        DATA INPUTS; 
            set INPUTS;
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
            label ScenarioIndex="Unique Scenario ID";
        RUN;

        /* Calculate Parameters form Macro Inputs Here - these are repeated as comments at the start of each model phase below */
			* calculated parameters used in model post-processing;
				%LET HOSP_RATE = %SYSEVALF(&Admission_Rate. * &DiagnosedRate.);
				%LET ICU_RATE = %SYSEVALF(&ICUPercent. * &DiagnosedRate.);
				%LET VENT_RATE = %SYSEVALF(&VentPErcent. * &DiagnosedRate.);
			* calculated parameters used in models;
				%LET I = %SYSEVALF(&KnownAdmits. / 
											&MarketSharePercent. / 
												(&Admission_Rate. * &DiagnosedRate.));
				%LET GAMMA = %SYSEVALF(1 / &RecoveryDays.);
				%IF &SIGMA. <= 0 %THEN %LET SIGMA = 0.00000001;
					%LET SIGMAINV = %SYSEVALF(1 / &SIGMA.);
				%LET BETA = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * (1 - &SocialDistancing.));
				%LET R_T = %SYSEVALF(&BETA. / &GAMMA. * &Population.);

				%IF %sysevalf(%superq(SocialDistancingChange)=,boolean)=0 %THEN %DO;
					%LET sdchangetitle=Adjust R0 (Date / Event / R0 / Social Distancing Shift):;
					%LET ISOChangeLoop = %SYSFUNC(countw(&SocialDistancingChange.,:));
					%DO j = 1 %TO &ISOChangeLoop;
						%LET SocialDistancingChange&j = %scan(&SocialDistancingChange.,&j,:);
						%LET ISOChangeDate&j = %scan(&ISOChangeDate.,&j,:);
						%LET ISOChangeEvent&j = %scan(&ISOChangeEvent.,&j,:);
						%LET ISOChangeWindow&j = %scan(&ISOChangeWindow.,&j,:);

						%LET BETAChange&j = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j));
						%IF &j = 1 %THEN %LET R_T_Change&j = %SYSEVALF(&R_T - &&BETAChange&j / &GAMMA. * &Population.);
						%ELSE %DO;
							%LET j2=%eval(&j-1);
							%LET R_T_Change&j = %SYSEVALF(&&R_T_Change&j2 - &&BETAChange&j / &GAMMA. * &Population.);
						%END;

						%LET sdchangetitle = &sdchangetitle. (%sysfunc(INPUTN(&&ISOChangeDate&j., date10.), date9.) / &&ISOChangeEvent&j / %SYSFUNC(round(&&R_T_Change&j,.01)) / %SYSEVALF(&&SocialDistancingChange&j.*100)%);
					%END; 
				%END;
				%ELSE %DO;
					%LET sdchangetitle=No Adjustment to R0 over time;
					%LET ISOChangeLoop = 0;
				%END;
				
        DATA SCENARIOS;
            set SCENARIOS sashelp.vmacro(in=i where=(scope='EASYRUN'));
            if name in ('SQLEXITCODE','SQLOBS','SQLOOPS','SQLRC','SQLXOBS','SQLXOPENERRS','SCENARIOINDEX_BASE','PULLLIB','SDCHANGETITLE','J','J2') then delete;
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
            if i then STAGE='MODEL';
        RUN;
    /* Check to see if SCENARIOS (this scenario) has already been run before in SCENARIOS dataset */
        %GLOBAL ScenarioExist;
        %IF %SYSFUNC(exist(&PULLLIB..scenarios)) %THEN %DO;
            PROC SQL noprint;
                /* has this scenario been run before - all the same parameters and value - no more and no less */
                select count(*) into :ScenarioExist from
                    (select t1.ScenarioIndex, t2.ScenarioIndex, t2.ScenarioSource, t2.ScenarioUser
                        from 
                            (select *, count(*) as cnt 
                                from work.SCENARIOS
                                where name not in ('SCENARIO','SCENARIOINDEX_BASE','SCENARIONAMEUNIQUE','SCENARIOINDEX','SCENARIOSOURCE','SCENARIOUSER','SCENPLOT','PLOTS')
                                group by ScenarioIndex, ScenarioSource, ScenarioUser) t1
                            join
                            (select * from &PULLLIB..SCENARIOS
                                where name not in ('SCENARIO','SCENARIOINDEX_BASE','SCENARIONAMEUNIQUE','SCENARIOINDEX','SCENARIOSOURCE','SCENARIOUSER','SCENPLOT','PLOTS')) t2
                            on t1.name=t2.name and t1.value=t2.value and t1.STAGE=t2.STAGE
                        group by t1.ScenarioIndex, t2.ScenarioIndex, t2.ScenarioSource, t2.ScenarioUser, t1.cnt
                        having count(*) = t1.cnt)
                ; 
            QUIT;
        %END; 
        %ELSE %DO; 
            %LET ScenarioExist = 0;
        %END;

    /* recall an existing scenario to SASWORK if it matched */
        %GLOBAL ScenarioIndex_recall ScenarioSource_recall ScenarioUser_recall ScenarioNameUnique_recall ScenarioName_recall;
        %IF &ScenarioExist = 0 %THEN %DO;
            PROC SQL noprint; select max(ScenarioIndex) into :ScenarioIndex from work.SCENARIOS; QUIT;
        %END;
        /*%ELSE %IF &PLOTS. = YES %THEN %DO;*/
        %ELSE %DO;
            /* what was a ScenarioIndex value that matched the requested scenario - store that in ScenarioIndex_recall ... */
            PROC SQL noprint; /* can this be combined with the similar code above that counts matching scenarios? */
				select t2.ScenarioIndex, t2.ScenarioSource, t2.ScenarioUser, t2.ScenarioNameUnique, t2.ScenarioName into :ScenarioIndex_recall, :ScenarioSource_recall, :ScenarioUser_recall, :ScenarioNameUnique_recall, :ScenarioName_recall from
                    (select t1.ScenarioIndex, t2.ScenarioIndex, t2.ScenarioSource, t2.ScenarioUser, t2.ScenarioNameUnique, t2.ScenarioName
                        from 
                            (select *, count(*) as cnt 
                                from work.SCENARIOS
                                where name not in ('SCENARIO','SCENARIOINDEX_BASE','SCENARIONAMEUNIQUE','SCENARIOINDEX','SCENARIOSOURCE','SCENARIOUSER','SCENPLOT','PLOTS')
                                group by ScenarioIndex) t1
                            join
                            (select * from &PULLLIB..SCENARIOS
                                where name not in ('SCENARIO','SCENARIOINDEX_BASE','SCENARIONAMEUNIQUE','SCENARIOINDEX','SCENARIOSOURCE','SCENARIOUSER','SCENPLOT','PLOTS')) t2
                            on t1.name=t2.name and t1.value=t2.value and t1.STAGE=t2.STAGE
                        group by t1.ScenarioIndex, t2.ScenarioIndex, t2.ScenarioSource, t2.ScenarioUser, t1.cnt
                        having count(*) = t1.cnt)
                ;
            QUIT;
            /* pull the current scenario data to work for plots below */
            data work.MODEL_FINAL; set &PULLLIB..MODEL_FINAL; where ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall."; run;
            data work.ds_seir;
                set &PULLLIB..MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SEIR with Data Step"; 
            run;


            data work.ds_sir;
                set &PULLLIB..MODEL_FINAL;
                where  ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SIR with Data Step"; 
            run;

            data work.tmodel_seir;
                set &PULLLIB..MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SEIR with PROC (T)MODEL"; 
            run;


            data work.tmodel_sir;
                set &PULLLIB..MODEL_FINAL;
                where  ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SIR with PROC (T)MODEL"; 
            run;

            data work.tmodel_seir_fit_i;
                set &PULLLIB..MODEL_FINAL;
                where  ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SEIR with PROC (T)MODEL-Fit R0"; 
            run;
            %LET ScenarioIndex = &ScenarioIndex_recall.;
        %END;

    /* Prepare to create request plots from input parameter plots= */
        %IF %UPCASE(&plots.) = YES %THEN %DO; %LET plots = YES; %END;
        %ELSE %DO; %LET plots = NO; %END;

	/*PROC TMODEL SEIR APPROACH*/
		/* these are the calculations for variables used from above:
			* calculated parameters used in model post-processing;
				%LET HOSP_RATE = %SYSEVALF(&Admission_Rate. * &DiagnosedRate.);
				%LET ICU_RATE = %SYSEVALF(&ICUPercent. * &DiagnosedRate.);
				%LET VENT_RATE = %SYSEVALF(&VentPErcent. * &DiagnosedRate.);
			* calculated parameters used in models;
				%LET I = %SYSEVALF(&KnownAdmits. / 
											&MarketSharePercent. / 
												(&Admission_Rate. * &DiagnosedRate.));
				%LET GAMMA = %SYSEVALF(1 / &RecoveryDays.);
				%IF &SIGMA. <= 0 %THEN %LET SIGMA = 0.00000001;
					%LET SIGMAINV = %SYSEVALF(1 / &SIGMA.);
				%LET BETA = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * (1 - &SocialDistancing.));
				%LET R_T = %SYSEVALF(&BETA. / &GAMMA. * &Population.);

				%IF %sysevalf(%superq(SocialDistancingChange)=,boolean)=0 %THEN %DO;
					%LET sdchangetitle=Adjust R0 (Date / Event / R0 / Social Distancing Shift):;
					%LET ISOChangeLoop = %SYSFUNC(countw(&SocialDistancingChange.,:));
					%DO j = 1 %TO &ISOChangeLoop;
						%LET SocialDistancingChange&j = %scan(&SocialDistancingChange.,&j,:);
						%LET ISOChangeDate&j = %scan(&ISOChangeDate.,&j,:);
						%LET ISOChangeEvent&j = %scan(&ISOChangeEvent.,&j,:);
						%LET ISOChangeWindow&j = %scan(&ISOChangeWindow.,&j,:);

						%LET BETAChange&j = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j));
						%IF &j = 1 %THEN %LET R_T_Change&j = %SYSEVALF(&R_T - &&BETAChange&j / &GAMMA. * &Population.);
						%ELSE %DO;
							%LET j2=%eval(&j-1);
							%LET R_T_Change&j = %SYSEVALF(&&R_T_Change&j2 - &&BETAChange&j / &GAMMA. * &Population.);
						%END;

						%LET sdchangetitle = &sdchangetitle. (%sysfunc(INPUTN(&&ISOChangeDate&j., date10.), date9.) / &&ISOChangeEvent&j / %SYSFUNC(round(&&R_T_Change&j,.01)) / %SYSEVALF(&&SocialDistancingChange&j.*100)%);
					%END; 
				%END;
				%ELSE %DO;
					%LET sdchangetitle=No Adjustment to R0 over time;
					%LET ISOChangeLoop = 0;
				%END;
						*/
		/* If this is a new scenario then run it */
    	%IF &ScenarioExist = 0 AND &HAVE_SASETS = YES %THEN %DO;
			/*DATA FOR PROC TMODEL APPROACHES*/
				DATA DINIT(Label="Initial Conditions of Simulation");  
                    S_N = &Population. - (&I. / &DiagnosedRate.) - &InitRecovered.;
                    E_N = &E.;
                    I_N = &I. / &DiagnosedRate.;
                    R_N = &InitRecovered.;
                    /* prevent range below zero on each loop */
                    DO SIGMAfraction = 0.8 TO 1.2 BY 0.1;
					SIGMAINV = 1/(SIGMAfraction*&SIGMA.);
					SIGMAfraction = round(SIGMAfraction,.00001);
					DO RECOVERYDAYSfraction = 0.8 TO 1.2 BY 0.1;
                    RECOVERYDAYS = RECOVERYDAYSfraction * &RecoveryDays;
					RECOVERYDAYSfraction = round(RECOVERYDAYSfraction,.00001);
                        DO SOCIALDfraction = -.1 TO .1 BY 0.025;
						SOCIALD = SOCIALDfraction + &SocialDistancing;
						SOCIALDfraction = round(SOCIALDfraction,.00001);
						IF SOCIALD >=0 and SOCIALD<=1 THEN DO; 
                                GAMMA = 1 / RECOVERYDAYS;
                                BETA = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
                                                &Population. * (1 - SOCIALD);
								%DO j = 1 %TO &ISOChangeLoop;
									BETAChange&j = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j);
								%END;
								SocialDistancing = SOCIALD;	
                                DO TIME = 0 TO &N_DAYS. by 1;
									%IF &ISOChangeLoop > 0 %THEN %DO j = 1 %TO &ISOChangeLoop;
										/* For each day in window make adjustment to SocialDistancing */
										IF  &&ISOChangeDate&j < &DAY_ZERO + TIME <= &&ISOChangeDate&j + &&ISOChangeWindow&j THEN SocialDistancing = SocialDistancing + &&SocialDistancingChange&j/&&ISOChangeWindow&j;
									%END;
                                    OUTPUT; 
                                END;
                            END;
                        END;
						END;
					END; 
				RUN;

			%IF &HAVE_V151 = YES %THEN %DO; PROC TMODEL DATA = DINIT NOPRINT; performance nthreads=4 bypriority=1 partpriority=1; %END;
			%ELSE %DO; PROC MODEL DATA = DINIT NOPRINT; %END;
				/* construct BETA with additive changes */
				%IF &ISOChangeLoop > 0 %THEN %DO;
					BETAv = BETA 
					%DO j = 1 %TO &ISOChangeLoop;
						%DO j2 = 1 %TO &&ISOChangeWindow&j;
							/* apply a BETAChange for each day after ISOChangeDate up to number of days in ISOChangeWindow */
							- (&DAY_ZERO + TIME > &&ISOChangeDate&j + &j2 - 1) * BETAChange&j
						%END;	
					%END;
					;
				%END;
				%ELSE %DO;
					BETAv = BETA;
				%END;
				/* DIFFERENTIAL EQUATIONS */ 
				/* a. Decrease in healthy susceptible persons through infections: number of encounters of (S,I)*TransmissionProb*/
				DERT.S_N = -BETAv*S_N*I_N;
				/* b. inflow from a. -Decrease in Exposed: alpha*e "promotion" inflow from E->I;*/
				DERT.E_N = BETAv*S_N*I_N - SIGMAINV*E_N;
				/* c. inflow from b. - outflow through recovery or death during illness*/
				DERT.I_N = SIGMAINV*E_N - GAMMA*I_N;
				/* d. Recovered and death humans through "promotion" inflow from c.*/
				DERT.R_N = GAMMA*I_N;           
				/* SOLVE THE EQUATIONS */ 
				SOLVE S_N E_N I_N R_N / TIME=TIME OUT = TMODEL_SEIR_SIM; 
                by SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
				id TIME SocialDistancing BETAv;
			RUN;
			QUIT;

			/* use the center point of the ranges for the requested scenario inputs */
			DATA TMODEL_SEIR_SIM;
				FORMAT ModelType $30. DATE ADMIT_DATE DATE9.;
				ModelType="SEIR with PROC (T)MODEL";
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
				RETAIN counter cumulative_sum_fatality cumulative_Sum_Market_Fatality;
				SET TMODEL_SEIR_SIM(RENAME=(TIME=DAY BETAv=BETA) DROP=_ERRORS_ _MODE_ _TYPE_ BETA);
				DAY = round(DAY,1);
                *WHERE SIGMAfraction=1 and RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				BY SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
					IF first.SOCIALDfraction THEN counter = 1;
					ELSE counter + 1;
				/* START: Common Post-Processing Across each Model Type and Approach */

					RT = BETA / GAMMA * &Population.;

					NEWINFECTED=LAG&IncubationPeriod(SUM(LAG(SUM(S_N,E_N)),-1*SUM(S_N,E_N)));
						IF counter < &IncubationPeriod THEN NEWINFECTED = .;
						IF NEWINFECTED < 0 THEN NEWINFECTED=0;

					HOSP = CEIL(NEWINFECTED * &HOSP_RATE. * &MarketSharePercent.);
					ICU = CEIL(NEWINFECTED * &ICU_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					VENT = CEIL(NEWINFECTED * &VENT_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					
					Fatality = CEIL(NEWINFECTED * &FatalityRate * &MarketSharePercent. * &HOSP_RATE.);
						Cumulative_sum_fatality + Fatality;
						Deceased_Today = Fatality;
						Total_Deaths = Cumulative_sum_fatality;
					
					MARKET_HOSP = CEIL(NEWINFECTED * &HOSP_RATE.);
					MARKET_ICU = CEIL(NEWINFECTED * &ICU_RATE. * &HOSP_RATE.);
					MARKET_VENT = CEIL(NEWINFECTED * &VENT_RATE. * &HOSP_RATE.);
					MARKET_ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &HOSP_RATE.);
					MARKET_DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &HOSP_RATE.);
					
					Market_Fatality = CEIL(NEWINFECTED * &FatalityRate. * &HOSP_RATE.);
						cumulative_Sum_Market_Fatality + Market_Fatality;
						Market_Deceased_Today = Market_Fatality;
						Market_Total_Deaths = cumulative_Sum_Market_Fatality;

					/* setup LOS macro variables - create *_LOS_TABLE string for rand('TABLED') call in _OCCUPANCY variable calculations */	
						%LET los_varlist = HOSP ICU VENT ECMO DIAL;
							%DO j = 1 %TO %sysfunc(countw(&los_varlist));
								/* pick a variable from &los_varlist to work on and add _LOS as suffix to name */
									%LET los_curvar = %scan(&los_varlist,&j)_LOS;
								/* store the number of days entered in &los_len */
									%LET los_len = %sysfunc(countw(&&&los_curvar,:));
								/* detect day|rate pairs and reconstruct &&&los_curvar as a : delimited list of rates for each day */
									%IF %sysfunc(countw(&&&los_curvar,|)) > 1 %THEN %DO;
										/* iterate over input pairs - assuming they are in order */
											%DO d = 1 %TO &los_len;
												/* caputure the starting day for this pair */
													%IF &d > 1 %THEN %LET d_day_start = %eval(&d_day+1);
													%ELSE %LET d_day_start = 1;
												/* extract the current value for day and rate */
													%LET d_day = %scan(%scan(&&&los_curvar,&d,:),1,|);
													%LET d_rate = %scan(%scan(&&&los_curvar,&d,:),2,|);
												/* iterate up to the day from the pair - fill in missing days with 0 rate */
													%DO e = &d_day_start %TO &d_day;
														/* initialize the string of rates on day 1 */
															%IF &e = 1 %THEN %DO;
																%IF &e < &d_day %THEN %LET out_str = 0;
																%ELSE %LET out_str = &d_rate;
															%END;
														/* increment the string of rates on day > 1 */
															%ELSE %DO;
																%IF &e < &d_day %THEN %LET out_str =  &out_str:0;
																%ELSE %LET out_str = &out_str:&d_rate;
															%END;
													%END;
											%END;
											/* update &los_curvar with the new string of rates */
												%let &los_curvar = %sysfunc(compress(&out_str));
											/* update &los_len to the length of the new unravled string */
												%let los_len = %sysfunc(countw(&&&los_curvar,:));
									%END;
								/* the user input a range or rates for LOS = 1, 2, ... */
								%IF &los_len > 1 %THEN %DO;
									/* initialize the *_LOS_TABLE macro variable with the day 1 rate */
										%LET &los_curvar._TABLE = %scan(&&&los_curvar,1,:);
									/* for each day from 2 to the last entered append the days rate with comma delimiter */
										%DO k = 2 %TO &los_len;
											%LET &los_curvar._TABLE = &&&los_curvar._TABLE,%scan(&&&los_curvar,&k,:);
										%END;
									/* The MARKET_ variables for LOS_TABLE are equal to the *_LOS_TABLE created above */
										%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									/* store the number of days in *LOS_MAX and MARKET_*_LOS_MAX */
										%LET &los_curvar._MAX = &los_len;
										%LET MARKET_&los_curvar._MAX = &los_len;
								%END;
								/* the user input an integer value for LOS */
								%ELSE %DO;
									%LET MARKET_&los_curvar = &&&los_curvar;
									%IF &&&los_curvar = 1 %THEN %LET &los_curvar._TABLE = 1;
									%ELSE %LET &los_curvar._TABLE = 0;
										%DO k = 2 %TO &&&los_curvar;
											%IF &k = &&&los_curvar %THEN %LET &los_curvar._TABLE = &&&los_curvar._TABLE,1;
											%ELSE %LET &los_curvar._TABLE = &&&los_curvar._TABLE,0;
										%END;
									%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									%LET &los_curvar._MAX = &&&los_curvar;
									%LET MARKET_&los_curvar._MAX = &&&los_curvar;
								%END;
								 /* %put &los_curvar &&&los_curvar &&&los_curvar._MAX &&&los_curvar._TABLE; */
							%END;

					/* setup drivers for OCCUPANCY variable calculations in this code */
						%LET varlist = HOSP ICU VENT ECMO DIAL MARKET_HOSP MARKET_ICU MARKET_VENT MARKET_ECMO MARKET_DIAL;

					/* *_OCCUPANCY variable calculations */
						call streaminit(2019); /* may need to move to main data step code = as long as it appears before rand function it works correctly */						
						%DO j = 1 %TO %sysfunc(countw(&varlist));
							/* get largest possible LOS for current variable - stored in setup LOS above (increase by 1 in case rates dont sum to exactly 1 */
							%LET maxlos = %eval(%sysfunc(cat(&,%scan(&varlist,&j),_LOS_MAX)) + 1);
							/* arrays to hold an retain the distribution of LOS for hospital census */
								array %scan(&varlist,&j)_los{1:&maxlos} _TEMPORARY_;
							/* at the start of each day reduce the LOS for each patient by 1 day */
								do k = 1 to &maxlos;
									if day = 0 then do;
										%scan(&varlist,&j)_los{k}=0;
									end;
									else do;
										if k < &maxlos then do;
											%scan(&varlist,&j)_los{k} = %scan(&varlist,&j)_los{k+1};
										end;
										else do;
											%scan(&varlist,&j)_los{k} = 0;
										end;
									end;
								end;
							/* distribute todays new admissions by LOS */
								do k = 1 to round(%scan(&varlist,&j),1);
									/*temp = %sysfunc(cat(&,%scan(&varlist,&j),_LOS));*/
									temp = rand('TABLED',%sysfunc(cat(&,%scan(&varlist,&j),_LOS_TABLE)));
									if temp<0 then temp=0;
									else if temp>&maxlos then temp=&maxlos;
									/* if stay (>=1) then put them in the LOS array */
									if temp>0 then %scan(&varlist,&j)_los{temp}+1;
								end;
								/* set the output variables equal to total census for current value of Day */
									%scan(&varlist,&j)_OCCUPANCY = sum(of %scan(&varlist,&j)_los{*});
						%END;
							/* correct name of hospital occupancy to expected output */
								rename HOSP_OCCUPANCY=HOSPITAL_OCCUPANCY MARKET_HOSP_OCCUPANCY=MARKET_HOSPITAL_OCCUPANCY;
							/* derived Occupancy values - calculated from renamed variables so remember to use old name (*hosp) which persist until data is written */
								MedSurgOccupancy=Hosp_Occupancy-ICU_Occupancy;
								Market_MEdSurg_Occupancy=Market_Hosp_Occupancy-MArket_ICU_Occupancy;
					
					/* date variables */
						DATE = &DAY_ZERO. + round(DAY,1);
						ADMIT_DATE = SUM(DATE, &IncubationPeriod.);
					
					/* ISOChangeEvent variable */
						FORMAT ISOChangeEvent $30.;
						%IF %sysevalf(%superq(ISOChangeDate)=,boolean)=0 %THEN %DO;
							%DO j = 1 %TO %SYSFUNC(countw(&ISOChangeDate.,:)); 
								IF DATE = &&ISOChangeDate&j THEN DO;
									ISOChangeEvent = "&&ISOChangeEvent&j";
									/* the values in EventY_Multiplier will get multiplied by Peak values later in the code */
									EventY_Multiplier = 1.1+MOD(&j,2)/10;
								END;
							%END;
						%END;
						%ELSE %DO;
							ISOChangeEvent = '';
							EventY_Multiplier = .;
						%END;

					/* clean up */
						drop k temp;

				/* END: Common Post-Processing Across each Model Type and Approach */
				DROP CUM: counter SIGMAINV GAMMA BETAChange:;
			RUN;

			DATA TMODEL_SEIR; 
				SET TMODEL_SEIR_SIM;
				WHERE SIGMAfraction=1 and RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				DROP SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
			RUN;

            PROC SQL noprint;
                create table TMODEL_SEIR as
                    select * from
                        (select * from work.TMODEL_SEIR) B 
                        left join
                        (select min(HOSPITAL_OCCUPANCY) as LOWER_HOSPITAL_OCCUPANCY, 
                                min(ICU_OCCUPANCY) as LOWER_ICU_OCCUPANCY, 
                                min(VENT_OCCUPANCY) as LOWER_VENT_OCCUPANCY, 
                                min(ECMO_OCCUPANCY) as LOWER_ECMO_OCCUPANCY, 
                                min(DIAL_OCCUPANCY) as LOWER_DIAL_OCCUPANCY,
                                max(HOSPITAL_OCCUPANCY) as UPPER_HOSPITAL_OCCUPANCY, 
                                max(ICU_OCCUPANCY) as UPPER_ICU_OCCUPANCY, 
                                max(VENT_OCCUPANCY) as UPPER_VENT_OCCUPANCY, 
                                max(ECMO_OCCUPANCY) as UPPER_ECMO_OCCUPANCY, 
                                max(DIAL_OCCUPANCY) as UPPER_DIAL_OCCUPANCY,
                                Date, ModelType, ScenarioIndex
                            from TMODEL_SEIR_SIM
                            group by Date, ModelType, ScenarioIndex
                        ) U 
                        on B.ModelType=U.ModelType and B.ScenarioIndex=U.ScenarioIndex and B.DATE=U.DATE
                    order by ScenarioIndex, ModelType, Date
                ;
                drop table TMODEL_SEIR_SIM;
            QUIT;

            PROC APPEND base=work.boemska_tmodel_seir data=TMODEL_SEIR; run;
			PROC APPEND base=work.MODEL_FINAL data=TMODEL_SEIR; run;
				PROC SQL; drop table TMODEL_SEIR; drop table DINIT; QUIT;
			
		%END;

		%IF &PLOTS. = YES AND &HAVE_SASETS = YES %THEN %DO;
			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SEIR with PROC (T)MODEL' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - PROC TMODEL SEIR Approach";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
				SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;

			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SEIR with PROC (T)MODEL' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - PROC TMODEL SEIR Approach With Uncertainty Bounds";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
				
                BAND x=DATE lower=LOWER_HOSPITAL_OCCUPANCY upper=UPPER_HOSPITAL_OCCUPANCY / fillattrs=(color=blue transparency=.8) name="b1";
                BAND x=DATE lower=LOWER_ICU_OCCUPANCY upper=UPPER_ICU_OCCUPANCY / fillattrs=(color=red transparency=.8) name="b2";
                BAND x=DATE lower=LOWER_VENT_OCCUPANCY upper=UPPER_VENT_OCCUPANCY / fillattrs=(color=green transparency=.8) name="b3";
                BAND x=DATE lower=LOWER_ECMO_OCCUPANCY upper=UPPER_ECMO_OCCUPANCY / fillattrs=(color=brown transparency=.8) name="b4";
                BAND x=DATE lower=LOWER_DIAL_OCCUPANCY upper=UPPER_DIAL_OCCUPANCY / fillattrs=(color=purple transparency=.8) name="b5";
                SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(color=blue THICKNESS=2) name="l1";
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(color=red THICKNESS=2) name="l2";
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(color=green THICKNESS=2) name="l3";
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(color=brown THICKNESS=2) name="l4";
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(color=purple THICKNESS=2) name="l5";
                keylegend "l1" "l2" "l3" "l4" "l5";
                
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;
		%END;
	/*PROC TMODEL SIR APPROACH*/
		/* these are the calculations for variables used from above:
			* calculated parameters used in model post-processing;
				%LET HOSP_RATE = %SYSEVALF(&Admission_Rate. * &DiagnosedRate.);
				%LET ICU_RATE = %SYSEVALF(&ICUPercent. * &DiagnosedRate.);
				%LET VENT_RATE = %SYSEVALF(&VentPErcent. * &DiagnosedRate.);
			* calculated parameters used in models;
				%LET I = %SYSEVALF(&KnownAdmits. / 
											&MarketSharePercent. / 
												(&Admission_Rate. * &DiagnosedRate.));
				%LET GAMMA = %SYSEVALF(1 / &RecoveryDays.);
				%IF &SIGMA. <= 0 %THEN %LET SIGMA = 0.00000001;
					%LET SIGMAINV = %SYSEVALF(1 / &SIGMA.);
				%LET BETA = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * (1 - &SocialDistancing.));
				%LET R_T = %SYSEVALF(&BETA. / &GAMMA. * &Population.);

				%IF %sysevalf(%superq(SocialDistancingChange)=,boolean)=0 %THEN %DO;
					%LET sdchangetitle=Adjust R0 (Date / Event / R0 / Social Distancing Shift):;
					%LET ISOChangeLoop = %SYSFUNC(countw(&SocialDistancingChange.,:));
					%DO j = 1 %TO &ISOChangeLoop;
						%LET SocialDistancingChange&j = %scan(&SocialDistancingChange.,&j,:);
						%LET ISOChangeDate&j = %scan(&ISOChangeDate.,&j,:);
						%LET ISOChangeEvent&j = %scan(&ISOChangeEvent.,&j,:);
						%LET ISOChangeWindow&j = %scan(&ISOChangeWindow.,&j,:);

						%LET BETAChange&j = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j));
						%IF &j = 1 %THEN %LET R_T_Change&j = %SYSEVALF(&R_T - &&BETAChange&j / &GAMMA. * &Population.);
						%ELSE %DO;
							%LET j2=%eval(&j-1);
							%LET R_T_Change&j = %SYSEVALF(&&R_T_Change&j2 - &&BETAChange&j / &GAMMA. * &Population.);
						%END;

						%LET sdchangetitle = &sdchangetitle. (%sysfunc(INPUTN(&&ISOChangeDate&j., date10.), date9.) / &&ISOChangeEvent&j / %SYSFUNC(round(&&R_T_Change&j,.01)) / %SYSEVALF(&&SocialDistancingChange&j.*100)%);
					%END; 
				%END;
				%ELSE %DO;
					%LET sdchangetitle=No Adjustment to R0 over time;
					%LET ISOChangeLoop = 0;
				%END;
						*/
		/* If this is a new scenario then run it */
    	%IF &ScenarioExist = 0 AND &HAVE_SASETS = YES %THEN %DO;
			/*DATA FOR PROC TMODEL APPROACHES*/
				DATA DINIT(Label="Initial Conditions of Simulation");  
                    S_N = &Population. - (&I. / &DiagnosedRate.) - &InitRecovered.;
                    E_N = &E.;
                    I_N = &I. / &DiagnosedRate.;
                    R_N = &InitRecovered.;
                    /* prevent range below zero on each loop */
                        DO RECOVERYDAYSfraction = 0.8 TO 1.2 BY 0.1;
						RECOVERYDAYS = RECOVERYDAYSfraction*&RecoveryDays;
						RECOVERYDAYSfraction = round(RECOVERYDAYSfraction,.00001);
							DO SOCIALDfraction = -.1 TO .1 BY 0.025;
							SOCIALD = SOCIALDfraction + &SocialDistancing;
							SOCIALDfraction = round(SOCIALDfraction,.00001);
							IF SOCIALD >=0 and SOCIALD<=1 THEN DO; 
                                GAMMA = 1 / RECOVERYDAYS;
                                BETA = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
                                                &Population. * (1 - SOCIALD);
								%DO j = 1 %TO &ISOChangeLoop;
									BETAChange&j = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j);
								%END;
								SocialDistancing = SOCIALD;								
								DO TIME = 0 TO &N_DAYS. by 1;
									%IF &ISOChangeLoop > 0 %THEN %DO j = 1 %TO &ISOChangeLoop;
										/* For each day in window make adjustment to SocialDistancing */
										IF  &&ISOChangeDate&j < &DAY_ZERO + TIME <= &&ISOChangeDate&j + &&ISOChangeWindow&j THEN SocialDistancing = SocialDistancing + &&SocialDistancingChange&j/&&ISOChangeWindow&j;
									%END;
									OUTPUT; 
								END;
                            END;
							END;
                        END;
				RUN;

			%IF &HAVE_V151 = YES %THEN %DO; PROC TMODEL DATA = DINIT NOPRINT; performance nthreads=4 bypriority=1 partpriority=1; %END;
			%ELSE %DO; PROC MODEL DATA = DINIT NOPRINT; %END;
				/* construct BETA with additive changes */
				%IF &ISOChangeLoop > 0 %THEN %DO;
					BETAv = BETA 
					%DO j = 1 %TO &ISOChangeLoop;
						%DO j2 = 1 %TO &&ISOChangeWindow&j;
							/* apply a BETAChange for each day after ISOChangeDate up to number of days in ISOChangeWindow */
							- (&DAY_ZERO + TIME > &&ISOChangeDate&j + &j2 - 1) * BETAChange&j
						%END;	
					%END;
					;
				%END;
				%ELSE %DO;
					BETAv = BETA;
				%END;
				/* DIFFERENTIAL EQUATIONS */ 
				/* a. Decrease in healthy susceptible persons through infections: number of encounters of (S,I)*TransmissionProb*/
				DERT.S_N = -BETAv*S_N*I_N;
				/* c. inflow from b. - outflow through recovery or death during illness*/
				DERT.I_N = BETAv*S_N*I_N - GAMMA*I_N;
				/* d. Recovered and death humans through "promotion" inflow from c.*/
				DERT.R_N = GAMMA*I_N;           
				/* SOLVE THE EQUATIONS */ 
				SOLVE S_N I_N R_N / TIME=TIME OUT = TMODEL_SIR_SIM; 
                by RECOVERYDAYSfraction SOCIALDfraction;
				id TIME SocialDistancing BETAv;
			RUN;
			QUIT;  

            /* use the center point of the ranges for the requested scenario inputs */
			DATA TMODEL_SIR_SIM;
				FORMAT ModelType $30. DATE ADMIT_DATE DATE9.;	
				ModelType="SIR with PROC (T)MODEL";
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
				RETAIN counter cumulative_sum_fatality cumulative_Sum_Market_Fatality;
				E_N = &E.;  /* placeholder for post-processing of SIR model */
				SET TMODEL_SIR_SIM(RENAME=(TIME=DAY BETAv=BETA) DROP=_ERRORS_ _MODE_ _TYPE_ BETA);
				DAY = round(DAY,1);
                *WHERE RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				BY RECOVERYDAYSfraction SOCIALDfraction;
					IF first.SOCIALDfraction THEN counter = 1;
					ELSE counter + 1;
				/* START: Common Post-Processing Across each Model Type and Approach */

					RT = BETA / GAMMA * &Population.;

					NEWINFECTED=LAG&IncubationPeriod(SUM(LAG(SUM(S_N,E_N)),-1*SUM(S_N,E_N)));
						IF counter < &IncubationPeriod THEN NEWINFECTED = .;
						IF NEWINFECTED < 0 THEN NEWINFECTED=0;

					HOSP = CEIL(NEWINFECTED * &HOSP_RATE. * &MarketSharePercent.);
					ICU = CEIL(NEWINFECTED * &ICU_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					VENT = CEIL(NEWINFECTED * &VENT_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					
					Fatality = CEIL(NEWINFECTED * &FatalityRate * &MarketSharePercent. * &HOSP_RATE.);
						Cumulative_sum_fatality + Fatality;
						Deceased_Today = Fatality;
						Total_Deaths = Cumulative_sum_fatality;
					
					MARKET_HOSP = CEIL(NEWINFECTED * &HOSP_RATE.);
					MARKET_ICU = CEIL(NEWINFECTED * &ICU_RATE. * &HOSP_RATE.);
					MARKET_VENT = CEIL(NEWINFECTED * &VENT_RATE. * &HOSP_RATE.);
					MARKET_ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &HOSP_RATE.);
					MARKET_DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &HOSP_RATE.);
					
					Market_Fatality = CEIL(NEWINFECTED * &FatalityRate. * &HOSP_RATE.);
						cumulative_Sum_Market_Fatality + Market_Fatality;
						Market_Deceased_Today = Market_Fatality;
						Market_Total_Deaths = cumulative_Sum_Market_Fatality;

					/* setup LOS macro variables - create *_LOS_TABLE string for rand('TABLED') call in _OCCUPANCY variable calculations */	
						%LET los_varlist = HOSP ICU VENT ECMO DIAL;
							%DO j = 1 %TO %sysfunc(countw(&los_varlist));
								/* pick a variable from &los_varlist to work on and add _LOS as suffix to name */
									%LET los_curvar = %scan(&los_varlist,&j)_LOS;
								/* store the number of days entered in &los_len */
									%LET los_len = %sysfunc(countw(&&&los_curvar,:));
								/* detect day|rate pairs and reconstruct &&&los_curvar as a : delimited list of rates for each day */
									%IF %sysfunc(countw(&&&los_curvar,|)) > 1 %THEN %DO;
										/* iterate over input pairs - assuming they are in order */
											%DO d = 1 %TO &los_len;
												/* caputure the starting day for this pair */
													%IF &d > 1 %THEN %LET d_day_start = %eval(&d_day+1);
													%ELSE %LET d_day_start = 1;
												/* extract the current value for day and rate */
													%LET d_day = %scan(%scan(&&&los_curvar,&d,:),1,|);
													%LET d_rate = %scan(%scan(&&&los_curvar,&d,:),2,|);
												/* iterate up to the day from the pair - fill in missing days with 0 rate */
													%DO e = &d_day_start %TO &d_day;
														/* initialize the string of rates on day 1 */
															%IF &e = 1 %THEN %DO;
																%IF &e < &d_day %THEN %LET out_str = 0;
																%ELSE %LET out_str = &d_rate;
															%END;
														/* increment the string of rates on day > 1 */
															%ELSE %DO;
																%IF &e < &d_day %THEN %LET out_str =  &out_str:0;
																%ELSE %LET out_str = &out_str:&d_rate;
															%END;
													%END;
											%END;
											/* update &los_curvar with the new string of rates */
												%let &los_curvar = %sysfunc(compress(&out_str));
											/* update &los_len to the length of the new unravled string */
												%let los_len = %sysfunc(countw(&&&los_curvar,:));
									%END;
								/* the user input a range or rates for LOS = 1, 2, ... */
								%IF &los_len > 1 %THEN %DO;
									/* initialize the *_LOS_TABLE macro variable with the day 1 rate */
										%LET &los_curvar._TABLE = %scan(&&&los_curvar,1,:);
									/* for each day from 2 to the last entered append the days rate with comma delimiter */
										%DO k = 2 %TO &los_len;
											%LET &los_curvar._TABLE = &&&los_curvar._TABLE,%scan(&&&los_curvar,&k,:);
										%END;
									/* The MARKET_ variables for LOS_TABLE are equal to the *_LOS_TABLE created above */
										%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									/* store the number of days in *LOS_MAX and MARKET_*_LOS_MAX */
										%LET &los_curvar._MAX = &los_len;
										%LET MARKET_&los_curvar._MAX = &los_len;
								%END;
								/* the user input an integer value for LOS */
								%ELSE %DO;
									%LET MARKET_&los_curvar = &&&los_curvar;
									%IF &&&los_curvar = 1 %THEN %LET &los_curvar._TABLE = 1;
									%ELSE %LET &los_curvar._TABLE = 0;
										%DO k = 2 %TO &&&los_curvar;
											%IF &k = &&&los_curvar %THEN %LET &los_curvar._TABLE = &&&los_curvar._TABLE,1;
											%ELSE %LET &los_curvar._TABLE = &&&los_curvar._TABLE,0;
										%END;
									%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									%LET &los_curvar._MAX = &&&los_curvar;
									%LET MARKET_&los_curvar._MAX = &&&los_curvar;
								%END;
								 /* %put &los_curvar &&&los_curvar &&&los_curvar._MAX &&&los_curvar._TABLE; */
							%END;

					/* setup drivers for OCCUPANCY variable calculations in this code */
						%LET varlist = HOSP ICU VENT ECMO DIAL MARKET_HOSP MARKET_ICU MARKET_VENT MARKET_ECMO MARKET_DIAL;

					/* *_OCCUPANCY variable calculations */
						call streaminit(2019); /* may need to move to main data step code = as long as it appears before rand function it works correctly */						
						%DO j = 1 %TO %sysfunc(countw(&varlist));
							/* get largest possible LOS for current variable - stored in setup LOS above (increase by 1 in case rates dont sum to exactly 1 */
							%LET maxlos = %eval(%sysfunc(cat(&,%scan(&varlist,&j),_LOS_MAX)) + 1);
							/* arrays to hold an retain the distribution of LOS for hospital census */
								array %scan(&varlist,&j)_los{1:&maxlos} _TEMPORARY_;
							/* at the start of each day reduce the LOS for each patient by 1 day */
								do k = 1 to &maxlos;
									if day = 0 then do;
										%scan(&varlist,&j)_los{k}=0;
									end;
									else do;
										if k < &maxlos then do;
											%scan(&varlist,&j)_los{k} = %scan(&varlist,&j)_los{k+1};
										end;
										else do;
											%scan(&varlist,&j)_los{k} = 0;
										end;
									end;
								end;
							/* distribute todays new admissions by LOS */
								do k = 1 to round(%scan(&varlist,&j),1);
									/*temp = %sysfunc(cat(&,%scan(&varlist,&j),_LOS));*/
									temp = rand('TABLED',%sysfunc(cat(&,%scan(&varlist,&j),_LOS_TABLE)));
									if temp<0 then temp=0;
									else if temp>&maxlos then temp=&maxlos;
									/* if stay (>=1) then put them in the LOS array */
									if temp>0 then %scan(&varlist,&j)_los{temp}+1;
								end;
								/* set the output variables equal to total census for current value of Day */
									%scan(&varlist,&j)_OCCUPANCY = sum(of %scan(&varlist,&j)_los{*});
						%END;
							/* correct name of hospital occupancy to expected output */
								rename HOSP_OCCUPANCY=HOSPITAL_OCCUPANCY MARKET_HOSP_OCCUPANCY=MARKET_HOSPITAL_OCCUPANCY;
							/* derived Occupancy values - calculated from renamed variables so remember to use old name (*hosp) which persist until data is written */
								MedSurgOccupancy=Hosp_Occupancy-ICU_Occupancy;
								Market_MEdSurg_Occupancy=Market_Hosp_Occupancy-MArket_ICU_Occupancy;
					
					/* date variables */
						DATE = &DAY_ZERO. + round(DAY,1);
						ADMIT_DATE = SUM(DATE, &IncubationPeriod.);
					
					/* ISOChangeEvent variable */
						FORMAT ISOChangeEvent $30.;
						%IF %sysevalf(%superq(ISOChangeDate)=,boolean)=0 %THEN %DO;
							%DO j = 1 %TO %SYSFUNC(countw(&ISOChangeDate.,:)); 
								IF DATE = &&ISOChangeDate&j THEN DO;
									ISOChangeEvent = "&&ISOChangeEvent&j";
									/* the values in EventY_Multiplier will get multiplied by Peak values later in the code */
									EventY_Multiplier = 1.1+MOD(&j,2)/10;
								END;
							%END;
						%END;
						%ELSE %DO;
							ISOChangeEvent = '';
							EventY_Multiplier = .;
						%END;

					/* clean up */
						drop k temp;

				/* END: Common Post-Processing Across each Model Type and Approach */
				DROP CUM: counter GAMMA BETAChange:;
			RUN;

			DATA TMODEL_SIR; 
				SET TMODEL_SIR_SIM;
				WHERE RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				DROP RECOVERYDAYSfraction SOCIALDfraction;
			RUN;

            PROC SQL noprint;
                create table TMODEL_SIR as
                    select * from
                        (select * from work.TMODEL_SIR) B 
                        left join
                        (select min(HOSPITAL_OCCUPANCY) as LOWER_HOSPITAL_OCCUPANCY, 
                                min(ICU_OCCUPANCY) as LOWER_ICU_OCCUPANCY, 
                                min(VENT_OCCUPANCY) as LOWER_VENT_OCCUPANCY, 
                                min(ECMO_OCCUPANCY) as LOWER_ECMO_OCCUPANCY, 
                                min(DIAL_OCCUPANCY) as LOWER_DIAL_OCCUPANCY,
                                max(HOSPITAL_OCCUPANCY) as UPPER_HOSPITAL_OCCUPANCY, 
                                max(ICU_OCCUPANCY) as UPPER_ICU_OCCUPANCY, 
                                max(VENT_OCCUPANCY) as UPPER_VENT_OCCUPANCY, 
                                max(ECMO_OCCUPANCY) as UPPER_ECMO_OCCUPANCY, 
                                max(DIAL_OCCUPANCY) as UPPER_DIAL_OCCUPANCY,
                                Date, ModelType, ScenarioIndex
                            from TMODEL_SIR_SIM
                            group by Date, ModelType, ScenarioIndex
                        ) U 
                        on B.ModelType=U.ModelType and B.ScenarioIndex=U.ScenarioIndex and B.DATE=U.DATE
                    order by ScenarioIndex, ModelType, Date
                ;
                drop table TMODEL_SIR_SIM;
            QUIT;

            PROC APPEND base=work.boemska_tmodel_sir data=TMODEL_SIR; run;
			PROC APPEND base=work.MODEL_FINAL data=TMODEL_SIR NOWARN FORCE; run;
				PROC SQL; drop table TMODEL_SIR; drop table DINIT; QUIT;
			
		%END;

		%IF &PLOTS. = YES AND &HAVE_SASETS = YES %THEN %DO;
			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SIR with PROC (T)MODEL' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - PROC TMODEL SIR Approach";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
				SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;

			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SIR with PROC (T)MODEL' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - PROC TMODEL SIR Approach With Uncertainty Bounds";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
					
                BAND x=DATE lower=LOWER_HOSPITAL_OCCUPANCY upper=UPPER_HOSPITAL_OCCUPANCY / fillattrs=(color=blue transparency=.8) name="b1";
                BAND x=DATE lower=LOWER_ICU_OCCUPANCY upper=UPPER_ICU_OCCUPANCY / fillattrs=(color=red transparency=.8) name="b2";
                BAND x=DATE lower=LOWER_VENT_OCCUPANCY upper=UPPER_VENT_OCCUPANCY / fillattrs=(color=green transparency=.8) name="b3";
                BAND x=DATE lower=LOWER_ECMO_OCCUPANCY upper=UPPER_ECMO_OCCUPANCY / fillattrs=(color=brown transparency=.8) name="b4";
                BAND x=DATE lower=LOWER_DIAL_OCCUPANCY upper=UPPER_DIAL_OCCUPANCY / fillattrs=(color=purple transparency=.8) name="b5";
                SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(color=blue THICKNESS=2) name="l1";
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(color=red THICKNESS=2) name="l2";
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(color=green THICKNESS=2) name="l3";
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(color=brown THICKNESS=2) name="l4";
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(color=purple THICKNESS=2) name="l5";
                keylegend "l1" "l2" "l3" "l4" "l5";
                
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;
		%END;
	/* DATA STEP APPROACH FOR SEIR */
		/* these are the calculations for variables used from above:
			* calculated parameters used in model post-processing;
				%LET HOSP_RATE = %SYSEVALF(&Admission_Rate. * &DiagnosedRate.);
				%LET ICU_RATE = %SYSEVALF(&ICUPercent. * &DiagnosedRate.);
				%LET VENT_RATE = %SYSEVALF(&VentPErcent. * &DiagnosedRate.);
			* calculated parameters used in models;
				%LET I = %SYSEVALF(&KnownAdmits. / 
											&MarketSharePercent. / 
												(&Admission_Rate. * &DiagnosedRate.));
				%LET GAMMA = %SYSEVALF(1 / &RecoveryDays.);
				%IF &SIGMA. <= 0 %THEN %LET SIGMA = 0.00000001;
					%LET SIGMAINV = %SYSEVALF(1 / &SIGMA.);
				%LET BETA = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * (1 - &SocialDistancing.));
				%LET R_T = %SYSEVALF(&BETA. / &GAMMA. * &Population.);

				%IF %sysevalf(%superq(SocialDistancingChange)=,boolean)=0 %THEN %DO;
					%LET sdchangetitle=Adjust R0 (Date / Event / R0 / Social Distancing Shift):;
					%LET ISOChangeLoop = %SYSFUNC(countw(&SocialDistancingChange.,:));
					%DO j = 1 %TO &ISOChangeLoop;
						%LET SocialDistancingChange&j = %scan(&SocialDistancingChange.,&j,:);
						%LET ISOChangeDate&j = %scan(&ISOChangeDate.,&j,:);
						%LET ISOChangeEvent&j = %scan(&ISOChangeEvent.,&j,:);
						%LET ISOChangeWindow&j = %scan(&ISOChangeWindow.,&j,:);

						%LET BETAChange&j = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j));
						%IF &j = 1 %THEN %LET R_T_Change&j = %SYSEVALF(&R_T - &&BETAChange&j / &GAMMA. * &Population.);
						%ELSE %DO;
							%LET j2=%eval(&j-1);
							%LET R_T_Change&j = %SYSEVALF(&&R_T_Change&j2 - &&BETAChange&j / &GAMMA. * &Population.);
						%END;

						%LET sdchangetitle = &sdchangetitle. (%sysfunc(INPUTN(&&ISOChangeDate&j., date10.), date9.) / &&ISOChangeEvent&j / %SYSFUNC(round(&&R_T_Change&j,.01)) / %SYSEVALF(&&SocialDistancingChange&j.*100)%);
					%END; 
				%END;
				%ELSE %DO;
					%LET sdchangetitle=No Adjustment to R0 over time;
					%LET ISOChangeLoop = 0;
				%END;
						*/
		/* If this is a new scenario then run it */
    	%IF &ScenarioExist = 0 %THEN %DO;
			DATA DS_SEIR_SIM;
				FORMAT DATE DATE9.;
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
				/* prevent range below zero on each loop */
				DO SIGMAfraction = 0.8 TO 1.2 BY 0.1;
					SIGMAINV = 1/(SIGMAfraction*&SIGMA.);
					SIGMAfraction = round(SIGMAfraction,.00001);
					DO RECOVERYDAYSfraction = 0.8 TO 1.2 BY 0.1;
                    RECOVERYDAYS = RECOVERYDAYSfraction * &RecoveryDays;
					RECOVERYDAYSfraction = round(RECOVERYDAYSfraction,.00001);
                        DO SOCIALDfraction = -.1 TO .1 BY 0.025;
						SOCIALD = SOCIALDfraction + &SocialDistancing;
						SOCIALDfraction = round(SOCIALDfraction,.00001);
						IF SOCIALD >=0 and SOCIALD<=1 THEN DO; 
							GAMMA = 1 / RECOVERYDAYS;
							kBETA = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
											&Population. * (1 - SOCIALD);
							%DO j = 1 %TO &ISOChangeLoop;
								BETAChange&j = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j);
							%END;				
							byinc = 0.1;
							DO DAY = 0 TO &N_DAYS. by byinc;
								IF DAY = 0 THEN DO;
									S_N = &Population. - (&I. / &DiagnosedRate.) - &InitRecovered.;
									E_N = &E.;
									I_N = &I. / &DiagnosedRate.;
									R_N = &InitRecovered.;
									BETA = kBETA;
										SocialDistancing = SOCIALD;
									N = SUM(S_N, E_N, I_N, R_N);
								END;
								ELSE DO;
									BETA = LAG_BETA;
									S_N = LAG_S - (BETA * LAG_S * LAG_I)*byinc;
									E_N = LAG_E + (BETA * LAG_S * LAG_I - SIGMAINV * LAG_E)*byinc;
									I_N = LAG_I + (SIGMAINV * LAG_E - GAMMA * LAG_I)*byinc;
									R_N = LAG_R + (GAMMA * LAG_I)*byinc;
									N = SUM(S_N, E_N, I_N, R_N);
									SCALE = LAG_N / N;
									IF S_N < 0 THEN S_N = 0;
									IF E_N < 0 THEN E_N = 0;
									IF I_N < 0 THEN I_N = 0;
									IF R_N < 0 THEN R_N = 0;
									S_N = SCALE*S_N;
									E_N = SCALE*E_N;
									I_N = SCALE*I_N;
									R_N = SCALE*R_N;
								END;
								/* prepare for tomorrow (DAY+1) */
									/* remember todays values for SEIR */
										LAG_S = S_N;
										LAG_E = E_N;
										LAG_I = I_N;
										LAG_R = R_N;
										LAG_N = N;
										LAG_BETA = BETA;
									/* output integer days and make BETA adjustments*/
										IF abs(DAY - round(DAY,1)) < byinc/10 THEN DO;
											DATE = &DAY_ZERO. + round(DAY,1); /* brought forward from post-processing: examine location impact on ISOChangeDate* */
											/* implement shifts in SocialDistancing on and over date ranges */
												%IF &ISOChangeLoop > 0 %THEN %DO;
													%DO j = 1 %TO &ISOChangeLoop;
														%IF &j > 1 %THEN %DO; ELSE %END;
															IF &&ISOChangeDate&j <= date < &&ISOChangeDate&j + &&ISOChangeWindow&j THEN DO;
																BETAChange = BETAChange&j.;
																SocialDistancing = SocialDistancing + &&SocialDistancingChange&j/&&ISOChangeWindow&j;
															END;
													%END;
													ELSE BETAChange = 0;
												%END;
												%ELSE %DO; BETAChange = 0; %END;
											/* adjust BETA for tomorrow */
												LAG_BETA = BETA - BETAChange;
											OUTPUT;
										END;
							END;
						END;
						END;
					END;
				END;
				DROP LAG: byinc kBETA BETAChange:;
			RUN;

			DATA DS_SEIR_SIM;
				FORMAT ModelType $30. DATE ADMIT_DATE DATE9.;		
				ModelType="SEIR with Data Step";
				RETAIN counter cumulative_sum_fatality cumulative_Sum_Market_Fatality;
				SET DS_SEIR_SIM;
				*WHERE SIGMAfraction=1 and RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				BY SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
					IF first.SOCIALDfraction THEN counter = 1;
					ELSE counter + 1;
				/* START: Common Post-Processing Across each Model Type and Approach */

					RT = BETA / GAMMA * &Population.;

					NEWINFECTED=LAG&IncubationPeriod(SUM(LAG(SUM(S_N,E_N)),-1*SUM(S_N,E_N)));
						IF counter < &IncubationPeriod THEN NEWINFECTED = .;
						IF NEWINFECTED < 0 THEN NEWINFECTED=0;

					HOSP = CEIL(NEWINFECTED * &HOSP_RATE. * &MarketSharePercent.);
					ICU = CEIL(NEWINFECTED * &ICU_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					VENT = CEIL(NEWINFECTED * &VENT_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					
					Fatality = CEIL(NEWINFECTED * &FatalityRate * &MarketSharePercent. * &HOSP_RATE.);
						Cumulative_sum_fatality + Fatality;
						Deceased_Today = Fatality;
						Total_Deaths = Cumulative_sum_fatality;
					
					MARKET_HOSP = CEIL(NEWINFECTED * &HOSP_RATE.);
					MARKET_ICU = CEIL(NEWINFECTED * &ICU_RATE. * &HOSP_RATE.);
					MARKET_VENT = CEIL(NEWINFECTED * &VENT_RATE. * &HOSP_RATE.);
					MARKET_ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &HOSP_RATE.);
					MARKET_DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &HOSP_RATE.);
					
					Market_Fatality = CEIL(NEWINFECTED * &FatalityRate. * &HOSP_RATE.);
						cumulative_Sum_Market_Fatality + Market_Fatality;
						Market_Deceased_Today = Market_Fatality;
						Market_Total_Deaths = cumulative_Sum_Market_Fatality;

					/* setup LOS macro variables - create *_LOS_TABLE string for rand('TABLED') call in _OCCUPANCY variable calculations */	
						%LET los_varlist = HOSP ICU VENT ECMO DIAL;
							%DO j = 1 %TO %sysfunc(countw(&los_varlist));
								/* pick a variable from &los_varlist to work on and add _LOS as suffix to name */
									%LET los_curvar = %scan(&los_varlist,&j)_LOS;
								/* store the number of days entered in &los_len */
									%LET los_len = %sysfunc(countw(&&&los_curvar,:));
								/* detect day|rate pairs and reconstruct &&&los_curvar as a : delimited list of rates for each day */
									%IF %sysfunc(countw(&&&los_curvar,|)) > 1 %THEN %DO;
										/* iterate over input pairs - assuming they are in order */
											%DO d = 1 %TO &los_len;
												/* caputure the starting day for this pair */
													%IF &d > 1 %THEN %LET d_day_start = %eval(&d_day+1);
													%ELSE %LET d_day_start = 1;
												/* extract the current value for day and rate */
													%LET d_day = %scan(%scan(&&&los_curvar,&d,:),1,|);
													%LET d_rate = %scan(%scan(&&&los_curvar,&d,:),2,|);
												/* iterate up to the day from the pair - fill in missing days with 0 rate */
													%DO e = &d_day_start %TO &d_day;
														/* initialize the string of rates on day 1 */
															%IF &e = 1 %THEN %DO;
																%IF &e < &d_day %THEN %LET out_str = 0;
																%ELSE %LET out_str = &d_rate;
															%END;
														/* increment the string of rates on day > 1 */
															%ELSE %DO;
																%IF &e < &d_day %THEN %LET out_str =  &out_str:0;
																%ELSE %LET out_str = &out_str:&d_rate;
															%END;
													%END;
											%END;
											/* update &los_curvar with the new string of rates */
												%let &los_curvar = %sysfunc(compress(&out_str));
											/* update &los_len to the length of the new unravled string */
												%let los_len = %sysfunc(countw(&&&los_curvar,:));
									%END;
								/* the user input a range or rates for LOS = 1, 2, ... */
								%IF &los_len > 1 %THEN %DO;
									/* initialize the *_LOS_TABLE macro variable with the day 1 rate */
										%LET &los_curvar._TABLE = %scan(&&&los_curvar,1,:);
									/* for each day from 2 to the last entered append the days rate with comma delimiter */
										%DO k = 2 %TO &los_len;
											%LET &los_curvar._TABLE = &&&los_curvar._TABLE,%scan(&&&los_curvar,&k,:);
										%END;
									/* The MARKET_ variables for LOS_TABLE are equal to the *_LOS_TABLE created above */
										%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									/* store the number of days in *LOS_MAX and MARKET_*_LOS_MAX */
										%LET &los_curvar._MAX = &los_len;
										%LET MARKET_&los_curvar._MAX = &los_len;
								%END;
								/* the user input an integer value for LOS */
								%ELSE %DO;
									%LET MARKET_&los_curvar = &&&los_curvar;
									%IF &&&los_curvar = 1 %THEN %LET &los_curvar._TABLE = 1;
									%ELSE %LET &los_curvar._TABLE = 0;
										%DO k = 2 %TO &&&los_curvar;
											%IF &k = &&&los_curvar %THEN %LET &los_curvar._TABLE = &&&los_curvar._TABLE,1;
											%ELSE %LET &los_curvar._TABLE = &&&los_curvar._TABLE,0;
										%END;
									%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									%LET &los_curvar._MAX = &&&los_curvar;
									%LET MARKET_&los_curvar._MAX = &&&los_curvar;
								%END;
								 /* %put &los_curvar &&&los_curvar &&&los_curvar._MAX &&&los_curvar._TABLE; */
							%END;

					/* setup drivers for OCCUPANCY variable calculations in this code */
						%LET varlist = HOSP ICU VENT ECMO DIAL MARKET_HOSP MARKET_ICU MARKET_VENT MARKET_ECMO MARKET_DIAL;

					/* *_OCCUPANCY variable calculations */
						call streaminit(2019); /* may need to move to main data step code = as long as it appears before rand function it works correctly */						
						%DO j = 1 %TO %sysfunc(countw(&varlist));
							/* get largest possible LOS for current variable - stored in setup LOS above (increase by 1 in case rates dont sum to exactly 1 */
							%LET maxlos = %eval(%sysfunc(cat(&,%scan(&varlist,&j),_LOS_MAX)) + 1);
							/* arrays to hold an retain the distribution of LOS for hospital census */
								array %scan(&varlist,&j)_los{1:&maxlos} _TEMPORARY_;
							/* at the start of each day reduce the LOS for each patient by 1 day */
								do k = 1 to &maxlos;
									if day = 0 then do;
										%scan(&varlist,&j)_los{k}=0;
									end;
									else do;
										if k < &maxlos then do;
											%scan(&varlist,&j)_los{k} = %scan(&varlist,&j)_los{k+1};
										end;
										else do;
											%scan(&varlist,&j)_los{k} = 0;
										end;
									end;
								end;
							/* distribute todays new admissions by LOS */
								do k = 1 to round(%scan(&varlist,&j),1);
									/*temp = %sysfunc(cat(&,%scan(&varlist,&j),_LOS));*/
									temp = rand('TABLED',%sysfunc(cat(&,%scan(&varlist,&j),_LOS_TABLE)));
									if temp<0 then temp=0;
									else if temp>&maxlos then temp=&maxlos;
									/* if stay (>=1) then put them in the LOS array */
									if temp>0 then %scan(&varlist,&j)_los{temp}+1;
								end;
								/* set the output variables equal to total census for current value of Day */
									%scan(&varlist,&j)_OCCUPANCY = sum(of %scan(&varlist,&j)_los{*});
						%END;
							/* correct name of hospital occupancy to expected output */
								rename HOSP_OCCUPANCY=HOSPITAL_OCCUPANCY MARKET_HOSP_OCCUPANCY=MARKET_HOSPITAL_OCCUPANCY;
							/* derived Occupancy values - calculated from renamed variables so remember to use old name (*hosp) which persist until data is written */
								MedSurgOccupancy=Hosp_Occupancy-ICU_Occupancy;
								Market_MEdSurg_Occupancy=Market_Hosp_Occupancy-MArket_ICU_Occupancy;
					
					/* date variables */
						DATE = &DAY_ZERO. + round(DAY,1);
						ADMIT_DATE = SUM(DATE, &IncubationPeriod.);
					
					/* ISOChangeEvent variable */
						FORMAT ISOChangeEvent $30.;
						%IF %sysevalf(%superq(ISOChangeDate)=,boolean)=0 %THEN %DO;
							%DO j = 1 %TO %SYSFUNC(countw(&ISOChangeDate.,:)); 
								IF DATE = &&ISOChangeDate&j THEN DO;
									ISOChangeEvent = "&&ISOChangeEvent&j";
									/* the values in EventY_Multiplier will get multiplied by Peak values later in the code */
									EventY_Multiplier = 1.1+MOD(&j,2)/10;
								END;
							%END;
						%END;
						%ELSE %DO;
							ISOChangeEvent = '';
							EventY_Multiplier = .;
						%END;

					/* clean up */
						drop k temp;

				/* END: Common Post-Processing Across each Model Type and Approach */
				DROP CUM: counter SIGMAINV RECOVERYDAYS SOCIALD GAMMA;
			RUN;

			DATA DS_SEIR;
				SET DS_SEIR_SIM;
				WHERE SIGMAfraction=1 and RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				DROP SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
			RUN;

		/* merge scenario data with uncertain bounds */
            PROC SQL noprint;
                create table DS_SEIR as
                    select * from
                        (select * from work.DS_SEIR) B 
                        left join
                        (select min(HOSPITAL_OCCUPANCY) as LOWER_HOSPITAL_OCCUPANCY, 
                                min(ICU_OCCUPANCY) as LOWER_ICU_OCCUPANCY, 
                                min(VENT_OCCUPANCY) as LOWER_VENT_OCCUPANCY, 
                                min(ECMO_OCCUPANCY) as LOWER_ECMO_OCCUPANCY, 
                                min(DIAL_OCCUPANCY) as LOWER_DIAL_OCCUPANCY,
                                max(HOSPITAL_OCCUPANCY) as UPPER_HOSPITAL_OCCUPANCY, 
                                max(ICU_OCCUPANCY) as UPPER_ICU_OCCUPANCY, 
                                max(VENT_OCCUPANCY) as UPPER_VENT_OCCUPANCY, 
                                max(ECMO_OCCUPANCY) as UPPER_ECMO_OCCUPANCY, 
                                max(DIAL_OCCUPANCY) as UPPER_DIAL_OCCUPANCY,
                                Date, ModelType, ScenarioIndex
                            from DS_SEIR_SIM
                            group by Date, ModelType, ScenarioIndex
                        ) U 
                        on B.ModelType=U.ModelType and B.ScenarioIndex=U.ScenarioIndex and B.DATE=U.DATE
                    order by ScenarioIndex, ModelType, Date
                ;
                drop table DS_SEIR_SIM;
            QUIT;

            PROC APPEND base=work.boemska_ds_seir data=DS_SEIR; run;
			PROC APPEND base=work.MODEL_FINAL data=DS_SEIR NOWARN FORCE; run;
				PROC SQL; drop table DS_SEIR; QUIT;

		%END;

		%IF &PLOTS. = YES %THEN %DO;
			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SEIR with Data Step' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - Data Step SEIR Approach";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
				SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;

			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SEIR with Data Step' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - Data Step SEIR Approach With Uncertainty Bounds";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
					
                BAND x=DATE lower=LOWER_HOSPITAL_OCCUPANCY upper=UPPER_HOSPITAL_OCCUPANCY / fillattrs=(color=blue transparency=.8) name="b1";
                BAND x=DATE lower=LOWER_ICU_OCCUPANCY upper=UPPER_ICU_OCCUPANCY / fillattrs=(color=red transparency=.8) name="b2";
                BAND x=DATE lower=LOWER_VENT_OCCUPANCY upper=UPPER_VENT_OCCUPANCY / fillattrs=(color=green transparency=.8) name="b3";
                BAND x=DATE lower=LOWER_ECMO_OCCUPANCY upper=UPPER_ECMO_OCCUPANCY / fillattrs=(color=brown transparency=.8) name="b4";
                BAND x=DATE lower=LOWER_DIAL_OCCUPANCY upper=UPPER_DIAL_OCCUPANCY / fillattrs=(color=purple transparency=.8) name="b5";
                SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(color=blue THICKNESS=2) name="l1";
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(color=red THICKNESS=2) name="l2";
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(color=green THICKNESS=2) name="l3";
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(color=brown THICKNESS=2) name="l4";
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(color=purple THICKNESS=2) name="l5";
                keylegend "l1" "l2" "l3" "l4" "l5";
                
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;
		%END;

	/* DATA STEP APPROACH FOR SIR */
		/* these are the calculations for variables used from above:
			* calculated parameters used in model post-processing;
				%LET HOSP_RATE = %SYSEVALF(&Admission_Rate. * &DiagnosedRate.);
				%LET ICU_RATE = %SYSEVALF(&ICUPercent. * &DiagnosedRate.);
				%LET VENT_RATE = %SYSEVALF(&VentPErcent. * &DiagnosedRate.);
			* calculated parameters used in models;
				%LET I = %SYSEVALF(&KnownAdmits. / 
											&MarketSharePercent. / 
												(&Admission_Rate. * &DiagnosedRate.));
				%LET GAMMA = %SYSEVALF(1 / &RecoveryDays.);
				%IF &SIGMA. <= 0 %THEN %LET SIGMA = 0.00000001;
					%LET SIGMAINV = %SYSEVALF(1 / &SIGMA.);
				%LET BETA = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * (1 - &SocialDistancing.));
				%LET R_T = %SYSEVALF(&BETA. / &GAMMA. * &Population.);

				%IF %sysevalf(%superq(SocialDistancingChange)=,boolean)=0 %THEN %DO;
					%LET sdchangetitle=Adjust R0 (Date / Event / R0 / Social Distancing Shift):;
					%LET ISOChangeLoop = %SYSFUNC(countw(&SocialDistancingChange.,:));
					%DO j = 1 %TO &ISOChangeLoop;
						%LET SocialDistancingChange&j = %scan(&SocialDistancingChange.,&j,:);
						%LET ISOChangeDate&j = %scan(&ISOChangeDate.,&j,:);
						%LET ISOChangeEvent&j = %scan(&ISOChangeEvent.,&j,:);
						%LET ISOChangeWindow&j = %scan(&ISOChangeWindow.,&j,:);

						%LET BETAChange&j = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j));
						%IF &j = 1 %THEN %LET R_T_Change&j = %SYSEVALF(&R_T - &&BETAChange&j / &GAMMA. * &Population.);
						%ELSE %DO;
							%LET j2=%eval(&j-1);
							%LET R_T_Change&j = %SYSEVALF(&&R_T_Change&j2 - &&BETAChange&j / &GAMMA. * &Population.);
						%END;

						%LET sdchangetitle = &sdchangetitle. (%sysfunc(INPUTN(&&ISOChangeDate&j., date10.), date9.) / &&ISOChangeEvent&j / %SYSFUNC(round(&&R_T_Change&j,.01)) / %SYSEVALF(&&SocialDistancingChange&j.*100)%);
					%END; 
				%END;
				%ELSE %DO;
					%LET sdchangetitle=No Adjustment to R0 over time;
					%LET ISOChangeLoop = 0;
				%END;
						*/
		/* If this is a new scenario then run it */
    	%IF &ScenarioExist = 0 %THEN %DO;
			DATA DS_SIR_SIM;
				FORMAT DATE DATE9.;
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
				/* prevent range below zero on each loop */
					DO RECOVERYDAYSfraction = 0.8 TO 1.2 BY 0.1;
                    RECOVERYDAYS = RECOVERYDAYSfraction*&RecoveryDays;
					RECOVERYDAYSfraction = round(RECOVERYDAYSfraction,.00001);
                        DO SOCIALDfraction = -.1 TO .1 BY 0.025;
						SOCIALD = SOCIALDfraction + &SocialDistancing;
						SOCIALDfraction = round(SOCIALDfraction,.00001);
						IF SOCIALD >=0 and SOCIALD<=1 THEN DO; 
							GAMMA = 1 / RECOVERYDAYS;
							kBETA = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
											&Population. * (1 - SOCIALD);
							%DO j = 1 %TO &ISOChangeLoop;
								BETAChange&j = ((2 ** (1 / &doublingtime.) - 1) + GAMMA) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j);
							%END;
							byinc = 0.1;
							DO DAY = 0 TO &N_DAYS. by byinc;
								IF DAY = 0 THEN DO;
									S_N = &Population. - (&I. / &DiagnosedRate.) - &InitRecovered.;
									I_N = &I./&DiagnosedRate.;
									R_N = &InitRecovered.;
									BETA = kBETA;
										SocialDistancing = SOCIALD;
									N = SUM(S_N, I_N, R_N);
								END;
								ELSE DO;
									BETA = LAG_BETA;
									S_N = LAG_S - (BETA * LAG_S * LAG_I)*byinc;
									I_N = LAG_I + (BETA * LAG_S * LAG_I - GAMMA * LAG_I)*byinc;
									R_N = LAG_R + (GAMMA * LAG_I)*byinc;
									N = SUM(S_N, I_N, R_N);
									SCALE = LAG_N / N;
									IF S_N < 0 THEN S_N = 0;
									IF I_N < 0 THEN I_N = 0;
									IF R_N < 0 THEN R_N = 0;
									S_N = SCALE*S_N;
									I_N = SCALE*I_N;
									R_N = SCALE*R_N;
								END;
								/* prepare for tomorrow (DAY+1) */
									/* remember todays values for SEIR */
										LAG_S = S_N;
										E_N = 0; LAG_E = E_N; /* placeholder for post-processing of SIR model */
										LAG_I = I_N;
										LAG_R = R_N;
										LAG_N = N;
										LAG_BETA = BETA;
									/* output integer days and make BETA adjustments */
										IF abs(DAY - round(DAY,1)) < byinc/10 THEN DO;
											DATE = &DAY_ZERO. + round(DAY,1); /* brought forward from post-processing: examine location impact on ISOChangeDate* */
											/* implement shifts in SocialDistancing on and over date ranges */
												%IF &ISOChangeLoop > 0 %THEN %DO;
													%DO j = 1 %TO &ISOChangeLoop;
														%IF &j > 1 %THEN %DO; ELSE %END;
															IF &&ISOChangeDate&j <= date < &&ISOChangeDate&j + &&ISOChangeWindow&j THEN DO;
																BETAChange = BETAChange&j.;
																SocialDistancing = SocialDistancing + &&SocialDistancingChange&j/&&ISOChangeWindow&j;
															END;
													%END;
													ELSE BETAChange = 0;
												%END;
												%ELSE %DO; BETAChange = 0; %END;
											/* adjust BETA for tomorrow */
												LAG_BETA = BETA - BETAChange;
											OUTPUT;
										END;
							END;
						END;
						END;
					END;
				DROP LAG: byinc kBETA BETAChange:;
			RUN;

		/* use the center point of the ranges for the request scenario inputs */
			DATA DS_SIR_SIM;
				FORMAT ModelType $30. DATE ADMIT_DATE DATE9.;		
				ModelType="SIR with Data Step";
				RETAIN counter cumulative_sum_fatality cumulative_Sum_Market_Fatality;
				SET DS_SIR_SIM;
				*WHERE RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				BY RECOVERYDAYSfraction SOCIALDfraction;
					IF first.SOCIALDfraction THEN counter = 1;
					ELSE counter + 1;
				/* START: Common Post-Processing Across each Model Type and Approach */

					RT = BETA / GAMMA * &Population.;

					NEWINFECTED=LAG&IncubationPeriod(SUM(LAG(SUM(S_N,E_N)),-1*SUM(S_N,E_N)));
						IF counter < &IncubationPeriod THEN NEWINFECTED = .;
						IF NEWINFECTED < 0 THEN NEWINFECTED=0;

					HOSP = CEIL(NEWINFECTED * &HOSP_RATE. * &MarketSharePercent.);
					ICU = CEIL(NEWINFECTED * &ICU_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					VENT = CEIL(NEWINFECTED * &VENT_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					
					Fatality = CEIL(NEWINFECTED * &FatalityRate * &MarketSharePercent. * &HOSP_RATE.);
						Cumulative_sum_fatality + Fatality;
						Deceased_Today = Fatality;
						Total_Deaths = Cumulative_sum_fatality;
					
					MARKET_HOSP = CEIL(NEWINFECTED * &HOSP_RATE.);
					MARKET_ICU = CEIL(NEWINFECTED * &ICU_RATE. * &HOSP_RATE.);
					MARKET_VENT = CEIL(NEWINFECTED * &VENT_RATE. * &HOSP_RATE.);
					MARKET_ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &HOSP_RATE.);
					MARKET_DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &HOSP_RATE.);
					
					Market_Fatality = CEIL(NEWINFECTED * &FatalityRate. * &HOSP_RATE.);
						cumulative_Sum_Market_Fatality + Market_Fatality;
						Market_Deceased_Today = Market_Fatality;
						Market_Total_Deaths = cumulative_Sum_Market_Fatality;

					/* setup LOS macro variables - create *_LOS_TABLE string for rand('TABLED') call in _OCCUPANCY variable calculations */	
						%LET los_varlist = HOSP ICU VENT ECMO DIAL;
							%DO j = 1 %TO %sysfunc(countw(&los_varlist));
								/* pick a variable from &los_varlist to work on and add _LOS as suffix to name */
									%LET los_curvar = %scan(&los_varlist,&j)_LOS;
								/* store the number of days entered in &los_len */
									%LET los_len = %sysfunc(countw(&&&los_curvar,:));
								/* detect day|rate pairs and reconstruct &&&los_curvar as a : delimited list of rates for each day */
									%IF %sysfunc(countw(&&&los_curvar,|)) > 1 %THEN %DO;
										/* iterate over input pairs - assuming they are in order */
											%DO d = 1 %TO &los_len;
												/* caputure the starting day for this pair */
													%IF &d > 1 %THEN %LET d_day_start = %eval(&d_day+1);
													%ELSE %LET d_day_start = 1;
												/* extract the current value for day and rate */
													%LET d_day = %scan(%scan(&&&los_curvar,&d,:),1,|);
													%LET d_rate = %scan(%scan(&&&los_curvar,&d,:),2,|);
												/* iterate up to the day from the pair - fill in missing days with 0 rate */
													%DO e = &d_day_start %TO &d_day;
														/* initialize the string of rates on day 1 */
															%IF &e = 1 %THEN %DO;
																%IF &e < &d_day %THEN %LET out_str = 0;
																%ELSE %LET out_str = &d_rate;
															%END;
														/* increment the string of rates on day > 1 */
															%ELSE %DO;
																%IF &e < &d_day %THEN %LET out_str =  &out_str:0;
																%ELSE %LET out_str = &out_str:&d_rate;
															%END;
													%END;
											%END;
											/* update &los_curvar with the new string of rates */
												%let &los_curvar = %sysfunc(compress(&out_str));
											/* update &los_len to the length of the new unravled string */
												%let los_len = %sysfunc(countw(&&&los_curvar,:));
									%END;
								/* the user input a range or rates for LOS = 1, 2, ... */
								%IF &los_len > 1 %THEN %DO;
									/* initialize the *_LOS_TABLE macro variable with the day 1 rate */
										%LET &los_curvar._TABLE = %scan(&&&los_curvar,1,:);
									/* for each day from 2 to the last entered append the days rate with comma delimiter */
										%DO k = 2 %TO &los_len;
											%LET &los_curvar._TABLE = &&&los_curvar._TABLE,%scan(&&&los_curvar,&k,:);
										%END;
									/* The MARKET_ variables for LOS_TABLE are equal to the *_LOS_TABLE created above */
										%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									/* store the number of days in *LOS_MAX and MARKET_*_LOS_MAX */
										%LET &los_curvar._MAX = &los_len;
										%LET MARKET_&los_curvar._MAX = &los_len;
								%END;
								/* the user input an integer value for LOS */
								%ELSE %DO;
									%LET MARKET_&los_curvar = &&&los_curvar;
									%IF &&&los_curvar = 1 %THEN %LET &los_curvar._TABLE = 1;
									%ELSE %LET &los_curvar._TABLE = 0;
										%DO k = 2 %TO &&&los_curvar;
											%IF &k = &&&los_curvar %THEN %LET &los_curvar._TABLE = &&&los_curvar._TABLE,1;
											%ELSE %LET &los_curvar._TABLE = &&&los_curvar._TABLE,0;
										%END;
									%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									%LET &los_curvar._MAX = &&&los_curvar;
									%LET MARKET_&los_curvar._MAX = &&&los_curvar;
								%END;
								 /* %put &los_curvar &&&los_curvar &&&los_curvar._MAX &&&los_curvar._TABLE; */
							%END;

					/* setup drivers for OCCUPANCY variable calculations in this code */
						%LET varlist = HOSP ICU VENT ECMO DIAL MARKET_HOSP MARKET_ICU MARKET_VENT MARKET_ECMO MARKET_DIAL;

					/* *_OCCUPANCY variable calculations */
						call streaminit(2019); /* may need to move to main data step code = as long as it appears before rand function it works correctly */						
						%DO j = 1 %TO %sysfunc(countw(&varlist));
							/* get largest possible LOS for current variable - stored in setup LOS above (increase by 1 in case rates dont sum to exactly 1 */
							%LET maxlos = %eval(%sysfunc(cat(&,%scan(&varlist,&j),_LOS_MAX)) + 1);
							/* arrays to hold an retain the distribution of LOS for hospital census */
								array %scan(&varlist,&j)_los{1:&maxlos} _TEMPORARY_;
							/* at the start of each day reduce the LOS for each patient by 1 day */
								do k = 1 to &maxlos;
									if day = 0 then do;
										%scan(&varlist,&j)_los{k}=0;
									end;
									else do;
										if k < &maxlos then do;
											%scan(&varlist,&j)_los{k} = %scan(&varlist,&j)_los{k+1};
										end;
										else do;
											%scan(&varlist,&j)_los{k} = 0;
										end;
									end;
								end;
							/* distribute todays new admissions by LOS */
								do k = 1 to round(%scan(&varlist,&j),1);
									/*temp = %sysfunc(cat(&,%scan(&varlist,&j),_LOS));*/
									temp = rand('TABLED',%sysfunc(cat(&,%scan(&varlist,&j),_LOS_TABLE)));
									if temp<0 then temp=0;
									else if temp>&maxlos then temp=&maxlos;
									/* if stay (>=1) then put them in the LOS array */
									if temp>0 then %scan(&varlist,&j)_los{temp}+1;
								end;
								/* set the output variables equal to total census for current value of Day */
									%scan(&varlist,&j)_OCCUPANCY = sum(of %scan(&varlist,&j)_los{*});
						%END;
							/* correct name of hospital occupancy to expected output */
								rename HOSP_OCCUPANCY=HOSPITAL_OCCUPANCY MARKET_HOSP_OCCUPANCY=MARKET_HOSPITAL_OCCUPANCY;
							/* derived Occupancy values - calculated from renamed variables so remember to use old name (*hosp) which persist until data is written */
								MedSurgOccupancy=Hosp_Occupancy-ICU_Occupancy;
								Market_MEdSurg_Occupancy=Market_Hosp_Occupancy-MArket_ICU_Occupancy;
					
					/* date variables */
						DATE = &DAY_ZERO. + round(DAY,1);
						ADMIT_DATE = SUM(DATE, &IncubationPeriod.);
					
					/* ISOChangeEvent variable */
						FORMAT ISOChangeEvent $30.;
						%IF %sysevalf(%superq(ISOChangeDate)=,boolean)=0 %THEN %DO;
							%DO j = 1 %TO %SYSFUNC(countw(&ISOChangeDate.,:)); 
								IF DATE = &&ISOChangeDate&j THEN DO;
									ISOChangeEvent = "&&ISOChangeEvent&j";
									/* the values in EventY_Multiplier will get multiplied by Peak values later in the code */
									EventY_Multiplier = 1.1+MOD(&j,2)/10;
								END;
							%END;
						%END;
						%ELSE %DO;
							ISOChangeEvent = '';
							EventY_Multiplier = .;
						%END;

					/* clean up */
						drop k temp;

				/* END: Common Post-Processing Across each Model Type and Approach */
				DROP CUM: counter RECOVERYDAYS SOCIALD GAMMA;
			RUN;

			DATA DS_SIR;
				SET DS_SIR_SIM;
				WHERE RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
				DROP RECOVERYDAYSfraction SOCIALDfraction;
			RUN;

		/* merge scenario data with uncertain bounds */
            PROC SQL noprint;
                create table DS_SIR as
                    select * from
                        (select * from work.DS_SIR) B 
                        left join
                        (select min(HOSPITAL_OCCUPANCY) as LOWER_HOSPITAL_OCCUPANCY, 
                                min(ICU_OCCUPANCY) as LOWER_ICU_OCCUPANCY, 
                                min(VENT_OCCUPANCY) as LOWER_VENT_OCCUPANCY, 
                                min(ECMO_OCCUPANCY) as LOWER_ECMO_OCCUPANCY, 
                                min(DIAL_OCCUPANCY) as LOWER_DIAL_OCCUPANCY,
                                max(HOSPITAL_OCCUPANCY) as UPPER_HOSPITAL_OCCUPANCY, 
                                max(ICU_OCCUPANCY) as UPPER_ICU_OCCUPANCY, 
                                max(VENT_OCCUPANCY) as UPPER_VENT_OCCUPANCY, 
                                max(ECMO_OCCUPANCY) as UPPER_ECMO_OCCUPANCY, 
                                max(DIAL_OCCUPANCY) as UPPER_DIAL_OCCUPANCY,
                                Date, ModelType, ScenarioIndex
                            from DS_SIR_SIM
                            group by Date, ModelType, ScenarioIndex
                        ) U 
                        on B.ModelType=U.ModelType and B.ScenarioIndex=U.ScenarioIndex and B.DATE=U.DATE
                    order by ScenarioIndex, ModelType, Date
                ;
                drop table DS_SIR_SIM;
            QUIT;

            PROC APPEND base=work.boemska_ds_sir data=DS_SIR; run;
			PROC APPEND base=work.MODEL_FINAL data=DS_SIR NOWARN FORCE; run;
				PROC SQL; drop table DS_SIR; QUIT;

		%END;

		%IF &PLOTS. = YES %THEN %DO;
			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SIR with Data Step' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - Data Step SIR Approach";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
				SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;

			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SIR with Data Step' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - Data Step SIR Approach With Uncertainty Bounds";
				TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
				TITLE3 "&sdchangetitle.";
					
                BAND x=DATE lower=LOWER_HOSPITAL_OCCUPANCY upper=UPPER_HOSPITAL_OCCUPANCY / fillattrs=(color=blue transparency=.8) name="b1";
                BAND x=DATE lower=LOWER_ICU_OCCUPANCY upper=UPPER_ICU_OCCUPANCY / fillattrs=(color=red transparency=.8) name="b2";
                BAND x=DATE lower=LOWER_VENT_OCCUPANCY upper=UPPER_VENT_OCCUPANCY / fillattrs=(color=green transparency=.8) name="b3";
                BAND x=DATE lower=LOWER_ECMO_OCCUPANCY upper=UPPER_ECMO_OCCUPANCY / fillattrs=(color=brown transparency=.8) name="b4";
                BAND x=DATE lower=LOWER_DIAL_OCCUPANCY upper=UPPER_DIAL_OCCUPANCY / fillattrs=(color=purple transparency=.8) name="b5";
                SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(color=blue THICKNESS=2) name="l1";
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(color=red THICKNESS=2) name="l2";
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(color=green THICKNESS=2) name="l3";
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(color=brown THICKNESS=2) name="l4";
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(color=purple THICKNESS=2) name="l5";
                keylegend "l1" "l2" "l3" "l4" "l5";
                
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3;
		%END;

	/* PROC TMODEL SEIR APPROACH - WITH OHIO FIT INTERVENE */
		/* these are the calculations for variables used from above:
			* calculated parameters used in model post-processing;
				%LET HOSP_RATE = %SYSEVALF(&Admission_Rate. * &DiagnosedRate.);
				%LET ICU_RATE = %SYSEVALF(&ICUPercent. * &DiagnosedRate.);
				%LET VENT_RATE = %SYSEVALF(&VentPErcent. * &DiagnosedRate.);
			* calculated parameters used in models;
				%LET I = %SYSEVALF(&KnownAdmits. / 
											&MarketSharePercent. / 
												(&Admission_Rate. * &DiagnosedRate.));
				%LET GAMMA = %SYSEVALF(1 / &RecoveryDays.);
				%IF &SIGMA. <= 0 %THEN %LET SIGMA = 0.00000001;
					%LET SIGMAINV = %SYSEVALF(1 / &SIGMA.);
				%LET BETA = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * (1 - &SocialDistancing.));
				%LET R_T = %SYSEVALF(&BETA. / &GAMMA. * &Population.);

				%IF %sysevalf(%superq(SocialDistancingChange)=,boolean)=0 %THEN %DO;
					%LET sdchangetitle=Adjust R0 (Date / Event / R0 / Social Distancing Shift):;
					%LET ISOChangeLoop = %SYSFUNC(countw(&SocialDistancingChange.,:));
					%DO j = 1 %TO &ISOChangeLoop;
						%LET SocialDistancingChange&j = %scan(&SocialDistancingChange.,&j,:);
						%LET ISOChangeDate&j = %scan(&ISOChangeDate.,&j,:);
						%LET ISOChangeEvent&j = %scan(&ISOChangeEvent.,&j,:);
						%LET ISOChangeWindow&j = %scan(&ISOChangeWindow.,&j,:);

						%LET BETAChange&j = %SYSEVALF(((2 ** (1 / &doublingtime.) - 1) + &GAMMA.) / 
												&Population. * ((&&SocialDistancingChange&j)/&&ISOChangeWindow&j));
						%IF &j = 1 %THEN %LET R_T_Change&j = %SYSEVALF(&R_T - &&BETAChange&j / &GAMMA. * &Population.);
						%ELSE %DO;
							%LET j2=%eval(&j-1);
							%LET R_T_Change&j = %SYSEVALF(&&R_T_Change&j2 - &&BETAChange&j / &GAMMA. * &Population.);
						%END;

						%LET sdchangetitle = &sdchangetitle. (%sysfunc(INPUTN(&&ISOChangeDate&j., date10.), date9.) / &&ISOChangeEvent&j / %SYSFUNC(round(&&R_T_Change&j,.01)) / %SYSEVALF(&&SocialDistancingChange&j.*100)%);
					%END; 
				%END;
				%ELSE %DO;
					%LET sdchangetitle=No Adjustment to R0 over time;
					%LET ISOChangeLoop = 0;
				%END;
						*/
		/* If this is a new scenario then run it */
    	%IF &ScenarioExist = 0 AND &HAVE_SASETS = YES AND %SYMEXIST(ISOChangeDate1) %THEN %DO;

			/* START FIT_INPUT - only if STORE.FIT_INPUT does not have data for yesterday or does not exist */
					%IF %sysfunc(exist(STORE.FIT_INPUT)) %THEN %DO;
						PROC SQL NOPRINT; 
							SELECT MIN(DATE) INTO :FIRST_CASE FROM STORE.FIT_INPUT;
							SELECT MAX(DATE) into :LATEST_CASE FROM STORE.FIT_INPUT; 
						QUIT;
					%END;
					%ELSE %DO;
						%LET LATEST_CASE=0;
					%END;
				/* update the fit source (STORE.FIT_INPUT) if outdated */
					%IF &LATEST_CASE. < %eval(%sysfunc(today())-2) %THEN %DO;

/* START: STORE.FIT_INPUT READ */

						/* import data feed from fit_import.csv in libname store */
							PROC IMPORT FILE="&homedir./fit_input.csv" OUT=STORE.FIT_INPUT DBMS=CSV REPLACE;
								GETNAMES = YES;
								DATAROW=2;
								GUESSINGROWS=200;
							RUN; 

							PROC SQL NOPRINT;
								SELECT MIN(DATE) INTO :FIRST_CASE FROM STORE.FIT_INPUT;
								SELECT MAX(DATE) INTO :LATEST_CASE FROM STORE.FIT_INPUT;
							QUIT;

						/* This section, from START: to END:, can be adapted to read case data from your data feed of choice

							If you have data in a database or a sas dataset you can use this section to read it and store it in STORE.FIT_INPUT

							Note: the condition before START: will only run a data refresh when the current source has no data for the last 2 days
						
							for an example of importing data from a source feed check out /examples/fit_import_ohio.sas
								the contents can be used to replace this section of the code from START: to END:
						*/
 
/* END: STORE.FIT_INPUT READ */

					%END;
            /* END FIT_INPUT **/
			/* Fit Model with Proc (t)Model (SAS/ETS) */
				%IF &HAVE_V151. = YES %THEN %DO; PROC TMODEL DATA = STORE.FIT_INPUT OUTMODEL=SEIRMOD_I NOPRINT; %END;
				%ELSE %DO; PROC MODEL DATA = STORE.FIT_INPUT OUTMODEL=SEIRMOD_I NOPRINT; %END;
					/* Parameters of interest */
					PARMS R0 &R_T. I0 &I. RI -1 DI &ISOChangeDate1.;
					BOUNDS 1 <= R0 <= 13;
					RESTRICT RI + R0 > 0;
					/* Fixed values */
					N = &Population.;
					INF = &RecoveryDays.;
					SIGMAINV = &SIGMAINV.;
					STEP = CDF('NORMAL',DATE, DI, 1);
					/* Differential equations */
					GAMMA = 1 / INF;
					BETA = (R0 + RI*STEP) * GAMMA / N;
					/* Differential equations */
					/* a. Decrease in healthy susceptible persons through infections: number of encounters of (S,I)*TransmissionProb*/
					DERT.S_N = -BETA * S_N * I_N;
					/* b. inflow from a. -Decrease in Exposed: alpha*e "promotion" inflow from E->I;*/
					DERT.E_N = BETA * S_N * I_N - SIGMAINV * E_N;
					/* c. inflow from b. - outflow through recovery or death during illness*/
					DERT.I_N = SIGMAINV * E_N - GAMMA * I_N;
					/* d. Recovered and death humans through "promotion" inflow from c.*/
					DERT.R_N = GAMMA * I_N;
					CUMULATIVE_CASE_COUNT = I_N + R_N;
					/* Fit the data */
					FIT CUMULATIVE_CASE_COUNT INIT=(S_N=&Population. E_N=0 I_N=I0 R_N=0) / TIME=TIME DYNAMIC OUTPREDICT OUTACTUAL OUT=FIT_PRED LTEBOUND=1E-10 OUTEST=FIT_PARMS
						%IF &HAVE_V151. = YES %THEN %DO; OPTIMIZER=ORMP(OPTTOL=1E-5) %END;;
					OUTVARS S_N E_N I_N R_N;
				QUIT;

			/* Prepare output: fit data and parameter data */
				DATA FIT_PRED;
					SET FIT_PRED;
					LABEL CUMULATIVE_CASE_COUNT='Cumulative Incidence';
					FORMAT ModelType $30. DATE DATE9.; 
					DATE = &FIRST_CASE. + TIME - 1;
					ModelType="SEIR with PROC (T)MODEL-Fit R0";
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
				run;
				DATA FIT_PARMS;
					SET FIT_PARMS;
					FORMAT ModelType $30.; 
					ModelType="SEIR with PROC (T)MODEL-Fit R0";
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
				run;

			/*Capture basline R0, date of Intervention effect, R0 after intervention*/
				PROC SQL NOPRINT;
					SELECT R0 INTO :R0_FIT FROM FIT_PARMS;
					SELECT "'"||PUT(DI,DATE9.)||"'"||"D" INTO :CURVEBEND1 FROM FIT_PARMS;
					SELECT SUM(R0,RI) INTO :R0_BEND_FIT FROM FIT_PARMS;
				QUIT;

			/*Calculate observed social distancing (and other interventions) percentage*/
				%LET SOC_DIST_FIT = %SYSEVALF(1 - &R0_BEND_FIT / &R0_FIT);

			/* DATA FOR PROC TMODEL APPROACHES */
				DATA DINIT(Label="Initial Conditions of Simulation");  
                    S_N = &Population. - (&I. / &DiagnosedRate.) - &InitRecovered.;
                    E_N = &E.;
                    I_N = &I. / &DiagnosedRate.;
                    R_N = &InitRecovered.;
                    /* prevent range below zero on each loop */
                    DO SIGMAfraction = 0.8 TO 1.2 BY 0.1;
					SIGMAINV = 1/(SIGMAfraction*&SIGMA.);
					SIGMAfraction = round(SIGMAfraction,.00001);
					DO RECOVERYDAYSfraction = 0.8 TO 1.2 BY 0.1;
                    RECOVERYDAYS = RECOVERYDAYSfraction * &RecoveryDays;
					RECOVERYDAYSfraction = round(RECOVERYDAYSfraction,.00001);
                        DO SOCIALDfraction = -.1 TO .1 BY 0.025;
						SOCIALD = SOCIALDfraction + &SocialDistancing;
						SOCIALDfraction = round(SOCIALDfraction,.00001);
						IF SOCIALD >=0 and SOCIALD<=1 THEN DO; 
                                GAMMA = 1 / RECOVERYDAYS;
								BETA = (&R0_FIT * GAMMA / &Population);
								/* relative change to BETA at CURVEBEND1 - amount of BETA removed */
								BETAChange1 = ((&R0_FIT - &R0_BEND_FIT) * GAMMA / &Population);
								SocialDistancing = 0;
                                DO TIME = 0 TO &N_DAYS. by 1;
									IF &DAY_ZERO + TIME > &CURVEBEND1 THEN SocialDistancing = &SOC_DIST_FIT;
                                    OUTPUT; 
                                END;
                            END;
                        END;
						END;
					END; 
				RUN;

			/* Create SEIR Projections based R0 and first social distancing change from model fit above, plus additional change points */
				%IF &HAVE_V151. = YES %THEN %DO; PROC TMODEL DATA=DINIT NOPRINT; %END;
				%ELSE %DO; PROC MODEL DATA=DINIT NOPRINT; %END;
					/* construct BETA with additive changes */
						BETAv = BETA - (&DAY_ZERO + TIME > &CURVEBEND1) * BETAChange1;
					/* DIFFERENTIAL EQUATIONS */ 
					/* a. Decrease in healthy susceptible persons through infections: number of encounters of (S,I)*TransmissionProb*/
					DERT.S_N = -BETAv*S_N*I_N;
					/* b. inflow from a. -Decrease in Exposed: alpha*e "promotion" inflow from E->I;*/
					DERT.E_N = BETAv*S_N*I_N - SIGMAINV*E_N;
					/* c. inflow from b. - outflow through recovery or death during illness*/
					DERT.I_N = SIGMAINV*E_N - GAMMA*I_N;
					/* d. Recovered and death humans through "promotion" inflow from c.*/
					DERT.R_N = GAMMA*I_N;           
					/* SOLVE THE EQUATIONS */ 
					SOLVE S_N E_N I_N R_N / OUT = TMODEL_SEIR_SIM_FIT_I;
					by SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
					id TIME SocialDistancing BETAv;
				RUN;
				QUIT;

				DATA TMODEL_SEIR_SIM_FIT_I;
					FORMAT ModelType $30. DATE ADMIT_DATE DATE9.;
					ModelType="SEIR with PROC (T)MODEL-Fit R0";
				FORMAT ScenarioName $50. ScenarioNameUnique $100. ScenarioSource $10. ScenarioUser $25.;
				ScenarioName="&Scenario.";
				ScenarioIndex=&ScenarioIndex.;
				ScenarioUser="&SYSUSERID.";
				ScenarioSource="&ScenarioSource.";
				ScenarioNameUnique=cats("&Scenario.",' (',ScenarioIndex,'-',"&SYSUSERID.",'-',"&ScenarioSource.",')');
					RETAIN counter cumulative_sum_fatality cumulative_Sum_Market_Fatality;
					SET TMODEL_SEIR_SIM_FIT_I(RENAME=(TIME=DAY BETAv=BETA) DROP=_ERRORS_ _MODE_ _TYPE_ BETA);
					DAY = round(DAY,1);
					*WHERE SIGMAfraction=1 and RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
					BY SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
						IF first.SOCIALDfraction THEN counter = 1;
						ELSE counter + 1;
				/* START: Common Post-Processing Across each Model Type and Approach */

					RT = BETA / GAMMA * &Population.;

					NEWINFECTED=LAG&IncubationPeriod(SUM(LAG(SUM(S_N,E_N)),-1*SUM(S_N,E_N)));
						IF counter < &IncubationPeriod THEN NEWINFECTED = .;
						IF NEWINFECTED < 0 THEN NEWINFECTED=0;

					HOSP = CEIL(NEWINFECTED * &HOSP_RATE. * &MarketSharePercent.);
					ICU = CEIL(NEWINFECTED * &ICU_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					VENT = CEIL(NEWINFECTED * &VENT_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &MarketSharePercent. * &HOSP_RATE.);
					
					Fatality = CEIL(NEWINFECTED * &FatalityRate * &MarketSharePercent. * &HOSP_RATE.);
						Cumulative_sum_fatality + Fatality;
						Deceased_Today = Fatality;
						Total_Deaths = Cumulative_sum_fatality;
					
					MARKET_HOSP = CEIL(NEWINFECTED * &HOSP_RATE.);
					MARKET_ICU = CEIL(NEWINFECTED * &ICU_RATE. * &HOSP_RATE.);
					MARKET_VENT = CEIL(NEWINFECTED * &VENT_RATE. * &HOSP_RATE.);
					MARKET_ECMO = CEIL(NEWINFECTED * &ECMO_RATE. * &HOSP_RATE.);
					MARKET_DIAL = CEIL(NEWINFECTED * &DIAL_RATE. * &HOSP_RATE.);
					
					Market_Fatality = CEIL(NEWINFECTED * &FatalityRate. * &HOSP_RATE.);
						cumulative_Sum_Market_Fatality + Market_Fatality;
						Market_Deceased_Today = Market_Fatality;
						Market_Total_Deaths = cumulative_Sum_Market_Fatality;

					/* setup LOS macro variables - create *_LOS_TABLE string for rand('TABLED') call in _OCCUPANCY variable calculations */	
						%LET los_varlist = HOSP ICU VENT ECMO DIAL;
							%DO j = 1 %TO %sysfunc(countw(&los_varlist));
								/* pick a variable from &los_varlist to work on and add _LOS as suffix to name */
									%LET los_curvar = %scan(&los_varlist,&j)_LOS;
								/* store the number of days entered in &los_len */
									%LET los_len = %sysfunc(countw(&&&los_curvar,:));
								/* detect day|rate pairs and reconstruct &&&los_curvar as a : delimited list of rates for each day */
									%IF %sysfunc(countw(&&&los_curvar,|)) > 1 %THEN %DO;
										/* iterate over input pairs - assuming they are in order */
											%DO d = 1 %TO &los_len;
												/* caputure the starting day for this pair */
													%IF &d > 1 %THEN %LET d_day_start = %eval(&d_day+1);
													%ELSE %LET d_day_start = 1;
												/* extract the current value for day and rate */
													%LET d_day = %scan(%scan(&&&los_curvar,&d,:),1,|);
													%LET d_rate = %scan(%scan(&&&los_curvar,&d,:),2,|);
												/* iterate up to the day from the pair - fill in missing days with 0 rate */
													%DO e = &d_day_start %TO &d_day;
														/* initialize the string of rates on day 1 */
															%IF &e = 1 %THEN %DO;
																%IF &e < &d_day %THEN %LET out_str = 0;
																%ELSE %LET out_str = &d_rate;
															%END;
														/* increment the string of rates on day > 1 */
															%ELSE %DO;
																%IF &e < &d_day %THEN %LET out_str =  &out_str:0;
																%ELSE %LET out_str = &out_str:&d_rate;
															%END;
													%END;
											%END;
											/* update &los_curvar with the new string of rates */
												%let &los_curvar = %sysfunc(compress(&out_str));
											/* update &los_len to the length of the new unravled string */
												%let los_len = %sysfunc(countw(&&&los_curvar,:));
									%END;
								/* the user input a range or rates for LOS = 1, 2, ... */
								%IF &los_len > 1 %THEN %DO;
									/* initialize the *_LOS_TABLE macro variable with the day 1 rate */
										%LET &los_curvar._TABLE = %scan(&&&los_curvar,1,:);
									/* for each day from 2 to the last entered append the days rate with comma delimiter */
										%DO k = 2 %TO &los_len;
											%LET &los_curvar._TABLE = &&&los_curvar._TABLE,%scan(&&&los_curvar,&k,:);
										%END;
									/* The MARKET_ variables for LOS_TABLE are equal to the *_LOS_TABLE created above */
										%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									/* store the number of days in *LOS_MAX and MARKET_*_LOS_MAX */
										%LET &los_curvar._MAX = &los_len;
										%LET MARKET_&los_curvar._MAX = &los_len;
								%END;
								/* the user input an integer value for LOS */
								%ELSE %DO;
									%LET MARKET_&los_curvar = &&&los_curvar;
									%IF &&&los_curvar = 1 %THEN %LET &los_curvar._TABLE = 1;
									%ELSE %LET &los_curvar._TABLE = 0;
										%DO k = 2 %TO &&&los_curvar;
											%IF &k = &&&los_curvar %THEN %LET &los_curvar._TABLE = &&&los_curvar._TABLE,1;
											%ELSE %LET &los_curvar._TABLE = &&&los_curvar._TABLE,0;
										%END;
									%LET MARKET_&los_curvar._TABLE = &&&los_curvar._TABLE;
									%LET &los_curvar._MAX = &&&los_curvar;
									%LET MARKET_&los_curvar._MAX = &&&los_curvar;
								%END;
								 /* %put &los_curvar &&&los_curvar &&&los_curvar._MAX &&&los_curvar._TABLE; */
							%END;

					/* setup drivers for OCCUPANCY variable calculations in this code */
						%LET varlist = HOSP ICU VENT ECMO DIAL MARKET_HOSP MARKET_ICU MARKET_VENT MARKET_ECMO MARKET_DIAL;

					/* *_OCCUPANCY variable calculations */
						call streaminit(2019); /* may need to move to main data step code = as long as it appears before rand function it works correctly */						
						%DO j = 1 %TO %sysfunc(countw(&varlist));
							/* get largest possible LOS for current variable - stored in setup LOS above (increase by 1 in case rates dont sum to exactly 1 */
							%LET maxlos = %eval(%sysfunc(cat(&,%scan(&varlist,&j),_LOS_MAX)) + 1);
							/* arrays to hold an retain the distribution of LOS for hospital census */
								array %scan(&varlist,&j)_los{1:&maxlos} _TEMPORARY_;
							/* at the start of each day reduce the LOS for each patient by 1 day */
								do k = 1 to &maxlos;
									if day = 0 then do;
										%scan(&varlist,&j)_los{k}=0;
									end;
									else do;
										if k < &maxlos then do;
											%scan(&varlist,&j)_los{k} = %scan(&varlist,&j)_los{k+1};
										end;
										else do;
											%scan(&varlist,&j)_los{k} = 0;
										end;
									end;
								end;
							/* distribute todays new admissions by LOS */
								do k = 1 to round(%scan(&varlist,&j),1);
									/*temp = %sysfunc(cat(&,%scan(&varlist,&j),_LOS));*/
									temp = rand('TABLED',%sysfunc(cat(&,%scan(&varlist,&j),_LOS_TABLE)));
									if temp<0 then temp=0;
									else if temp>&maxlos then temp=&maxlos;
									/* if stay (>=1) then put them in the LOS array */
									if temp>0 then %scan(&varlist,&j)_los{temp}+1;
								end;
								/* set the output variables equal to total census for current value of Day */
									%scan(&varlist,&j)_OCCUPANCY = sum(of %scan(&varlist,&j)_los{*});
						%END;
							/* correct name of hospital occupancy to expected output */
								rename HOSP_OCCUPANCY=HOSPITAL_OCCUPANCY MARKET_HOSP_OCCUPANCY=MARKET_HOSPITAL_OCCUPANCY;
							/* derived Occupancy values - calculated from renamed variables so remember to use old name (*hosp) which persist until data is written */
								MedSurgOccupancy=Hosp_Occupancy-ICU_Occupancy;
								Market_MEdSurg_Occupancy=Market_Hosp_Occupancy-MArket_ICU_Occupancy;
					
					/* date variables */
						DATE = &DAY_ZERO. + round(DAY,1);
						ADMIT_DATE = SUM(DATE, &IncubationPeriod.);
					
					/* ISOChangeEvent variable */
						FORMAT ISOChangeEvent $30.;
						%IF %sysevalf(%superq(ISOChangeDate)=,boolean)=0 %THEN %DO;
							%DO j = 1 %TO %SYSFUNC(countw(&ISOChangeDate.,:)); 
								IF DATE = &&ISOChangeDate&j THEN DO;
									ISOChangeEvent = "&&ISOChangeEvent&j";
									/* the values in EventY_Multiplier will get multiplied by Peak values later in the code */
									EventY_Multiplier = 1.1+MOD(&j,2)/10;
								END;
							%END;
						%END;
						%ELSE %DO;
							ISOChangeEvent = '';
							EventY_Multiplier = .;
						%END;

					/* clean up */
						drop k temp;

				/* END: Common Post-Processing Across each Model Type and Approach */
					DROP CUM: counter SIGMAINV GAMMA BETAChange:;
				RUN;

				DATA TMODEL_SEIR_FIT_I; 
					SET TMODEL_SEIR_SIM_FIT_I;
					WHERE SIGMAfraction=1 and RECOVERYDAYSfraction=1 and SOCIALDfraction=0;
					DROP SIGMAfraction RECOVERYDAYSfraction SOCIALDfraction;
				RUN;

				PROC SQL noprint;
					create table TMODEL_SEIR_FIT_I as
						select * from
							(select * from work.TMODEL_SEIR_FIT_I) B 
							left join
							(select min(HOSPITAL_OCCUPANCY) as LOWER_HOSPITAL_OCCUPANCY, 
									min(ICU_OCCUPANCY) as LOWER_ICU_OCCUPANCY, 
									min(VENT_OCCUPANCY) as LOWER_VENT_OCCUPANCY, 
									min(ECMO_OCCUPANCY) as LOWER_ECMO_OCCUPANCY, 
									min(DIAL_OCCUPANCY) as LOWER_DIAL_OCCUPANCY,
									max(HOSPITAL_OCCUPANCY) as UPPER_HOSPITAL_OCCUPANCY, 
									max(ICU_OCCUPANCY) as UPPER_ICU_OCCUPANCY, 
									max(VENT_OCCUPANCY) as UPPER_VENT_OCCUPANCY, 
									max(ECMO_OCCUPANCY) as UPPER_ECMO_OCCUPANCY, 
									max(DIAL_OCCUPANCY) as UPPER_DIAL_OCCUPANCY,
									Date, ModelType, ScenarioIndex
								from TMODEL_SEIR_SIM_FIT_I
								group by Date, ModelType, ScenarioIndex
							) U 
							on B.ModelType=U.ModelType and B.ScenarioIndex=U.ScenarioIndex and B.DATE=U.DATE
						order by ScenarioIndex, ModelType, Date
					;
					drop table TMODEL_SEIR_SIM_FIT_I;
				QUIT;

                PROC APPEND base=work.boemska_tmodel_seir_fit_i data=TMODEL_SEIR_FIT_I; run;
				PROC APPEND base=work.MODEL_FINAL data=TMODEL_SEIR_FIT_I NOWARN FORCE; run;
					PROC SQL; 
						drop table TMODEL_SEIR_FIT_I;
						drop table DINIT;
						drop table SEIRMOD_I;
					QUIT;

		%END;

		%IF &PLOTS. = YES AND &HAVE_SASETS = YES AND %SYMEXIST(ISOChangeDate1) %THEN %DO;

			%IF &ScenarioExist ~= 0 %THEN %DO;
				/* this is only needed to define macro varibles if the fit is being recalled.  
					If it is being run above these will already be defined */
					/*Capture basline R0, date of Intervention effect, R0 after intervention*/
						PROC SQL NOPRINT;
							SELECT R0 INTO :R0_FIT FROM FIT_PARMS;
							SELECT "'"||PUT(DI,DATE9.)||"'"||"D" INTO :CURVEBEND1 FROM FIT_PARMS;
							SELECT SUM(R0,RI) INTO :R0_BEND_FIT FROM FIT_PARMS;
						QUIT;

					/*Calculate observed social distancing (and other interventions) percentage*/
						%LET SOC_DIST_FIT = %SYSEVALF(1 - &R0_BEND_FIT / &R0_FIT);
			%END;

			/* Plot Fit of Actual v. Predicted */
			PROC SGPLOT DATA=work.FIT_PRED;
				WHERE _TYPE_  NE 'RESIDUAL' and ModelType='SEIR with PROC (T)MODEL-Fit R0' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Actual v. Predicted Infections in Region";
				TITLE2 "Initial R0: %SYSFUNC(round(&R0_FIT.,.01))";
				TITLE3 "Adjusted R0 after %sysfunc(INPUTN(&CURVEBEND1., date10.), date9.): %SYSFUNC(round(&R0_BEND_FIT.,.01)) with Social Distancing of %SYSFUNC(round(%SYSEVALF(&SOC_DIST_FIT.*100)))%";
				SERIES X=DATE Y=CUMULATIVE_CASE_COUNT / LINEATTRS=(THICKNESS=2) GROUP=_TYPE_  MARKERS NAME="cases";
				FORMAT CUMULATIVE_CASE_COUNT COMMA10.;
			RUN;
			TITLE;TITLE2;TITLE3;

			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SEIR with PROC (T)MODEL-Fit R0' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - PROC TMODEL SEIR Fit Approach";
				TITLE2 "Scenario: &Scenario., Initial Observed R0: %SYSFUNC(round(&R0_FIT.,.01))";
				TITLE3 "Adjusted Observed R0 after %sysfunc(INPUTN(&CURVEBEND1., date10.), date9.): %SYSFUNC(round(&R0_BEND_FIT.,.01)) with Observed Social Distancing of %SYSFUNC(round(%SYSEVALF(&SOC_DIST_FIT.*100)))%";
				TITLE4 "&sdchangetitle.";
				SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(THICKNESS=2);
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3; TITLE4;

			PROC SGPLOT DATA=work.MODEL_FINAL;
				where ModelType='SEIR with PROC (T)MODEL-Fit R0' and ScenarioIndex=&ScenarioIndex.;
				TITLE "Daily Occupancy - PROC TMODEL SEIR Fit Approach With Uncertainty Bounds";
				TITLE2 "Scenario: &Scenario., Initial Observed R0: %SYSFUNC(round(&R0_FIT.,.01))";
				TITLE3 "Adjusted Observed R0 after %sysfunc(INPUTN(&CURVEBEND1., date10.), date9.): %SYSFUNC(round(&R0_BEND_FIT.,.01)) with Observed Social Distancing of %SYSFUNC(round(%SYSEVALF(&SOC_DIST_FIT.*100)))%";
				TITLE4 "&sdchangetitle.";
				
                BAND x=DATE lower=LOWER_HOSPITAL_OCCUPANCY upper=UPPER_HOSPITAL_OCCUPANCY / fillattrs=(color=blue transparency=.8) name="b1";
                BAND x=DATE lower=LOWER_ICU_OCCUPANCY upper=UPPER_ICU_OCCUPANCY / fillattrs=(color=red transparency=.8) name="b2";
                BAND x=DATE lower=LOWER_VENT_OCCUPANCY upper=UPPER_VENT_OCCUPANCY / fillattrs=(color=green transparency=.8) name="b3";
                BAND x=DATE lower=LOWER_ECMO_OCCUPANCY upper=UPPER_ECMO_OCCUPANCY / fillattrs=(color=brown transparency=.8) name="b4";
                BAND x=DATE lower=LOWER_DIAL_OCCUPANCY upper=UPPER_DIAL_OCCUPANCY / fillattrs=(color=purple transparency=.8) name="b5";
                SERIES X=DATE Y=HOSPITAL_OCCUPANCY / LINEATTRS=(color=blue THICKNESS=2) name="l1";
				SERIES X=DATE Y=ICU_OCCUPANCY / LINEATTRS=(color=red THICKNESS=2) name="l2";
				SERIES X=DATE Y=VENT_OCCUPANCY / LINEATTRS=(color=green THICKNESS=2) name="l3";
				SERIES X=DATE Y=ECMO_OCCUPANCY / LINEATTRS=(color=brown THICKNESS=2) name="l4";
				SERIES X=DATE Y=DIAL_OCCUPANCY / LINEATTRS=(color=purple THICKNESS=2) name="l5";
                keylegend "l1" "l2" "l3" "l4" "l5";
                
				XAXIS LABEL="Date";
				YAXIS LABEL="Daily Occupancy";
			RUN;
			TITLE; TITLE2; TITLE3; TITLE4;

		%END;

    %IF &PLOTS. = YES %THEN %DO;
        /* if multiple models for a single scenarioIndex then plot them */
        PROC SQL noprint;
            select count(*) into :scenplot from (select distinct ModelType from work.MODEL_FINAL where ScenarioIndex=&ScenarioIndex.);
        QUIT;
        %IF &scenplot > 1 %THEN %DO;
            PROC SGPLOT DATA=work.MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex.;
                TITLE "Daily Hospital Occupancy - All Approaches";
                TITLE2 "Scenario: &Scenario., Initial R0: %SYSFUNC(round(&R_T.,.01)) with Initial Social Distancing of %SYSEVALF(&SocialDistancing.*100)%";
                TITLE3 "&sdchangetitle.";
                SERIES X=DATE Y=HOSPITAL_OCCUPANCY / GROUP=MODELTYPE LINEATTRS=(THICKNESS=2);
                XAXIS LABEL="Date";
                YAXIS LABEL="Daily Occupancy";
            RUN;
            TITLE; TITLE2; TITLE3;

            PROC SGPANEL DATA=work.MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex.;
				PANELBY MODELTYPE / NOVARNAME;
                TITLE "BETA Parameter Over Time - All Approaches";
                TITLE2 "&sdchangetitle.";
                SERIES X=DATE Y=BETA / GROUP=MODELTYPE LINEATTRS=(THICKNESS=2);
                COLAXIS LABEL="Date" GRID;
                ROWAXIS LABEL="BETA Parameter" GRID;
                %IF &ISOChangeLoop > 0 %THEN %DO;
                    REFLINE %DO j=1 %TO &ISOChangeLoop; &&ISOChangeDate&j %END; / axis=x ;
                %END;
            RUN;
            TITLE; TITLE2;

            PROC SGPANEL DATA=work.MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex.;
				PANELBY MODELTYPE / NOVARNAME;
                TITLE "Social Distancing Input Over Time - All Approaches";
                TITLE2 "&sdchangetitle.";
                SERIES X=DATE Y=SocialDistancing / GROUP=MODELTYPE LINEATTRS=(THICKNESS=2);
                COLAXIS LABEL="Date" GRID;
                ROWAXIS LABEL="Social Distancing" GRID;
                %IF &ISOChangeLoop > 0 %THEN %DO;
                    REFLINE %DO j=1 %TO &ISOChangeLoop; &&ISOChangeDate&j %END; / axis=x ;
                %END;
            RUN;
            TITLE; TITLE2;


            PROC SGPANEL DATA=work.MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex.;
				PANELBY MODELTYPE / NOVARNAME;
                TITLE "Reproduction Number Over Time - All Approaches";
                TITLE2 "&sdchangetitle.";
                SERIES X=DATE Y=RT / GROUP=MODELTYPE LINEATTRS=(THICKNESS=2);
                COLAXIS LABEL="Date" GRID;
                ROWAXIS LABEL="Reproduction Number" GRID;
                %IF &ISOChangeLoop > 0 %THEN %DO;
                    REFLINE %DO j=1 %TO &ISOChangeLoop; &&ISOChangeDate&j %END; / axis=x ;
                %END;
            RUN;
            TITLE; TITLE2;

        %END;	
    %END;

    /* code to manage output tables in STORE and CAS table management (coming soon) */
        %IF &ScenarioExist = 0 %THEN %DO;

				/*CREATE FLAGS FOR DAYS WITH PEAK VALUES OF DIFFERENT METRICS*/
					PROC SQL noprint;
						CREATE TABLE work.MODEL_FINAL AS
							SELECT MF.*, HOSP.PEAK_HOSPITAL_OCCUPANCY, ICU.PEAK_ICU_OCCUPANCY, VENT.PEAK_VENT_OCCUPANCY, 
								ECMO.PEAK_ECMO_OCCUPANCY, DIAL.PEAK_DIAL_OCCUPANCY, I.PEAK_I_N, FATAL.PEAK_FATALITY
							FROM work.MODEL_FINAL MF
								LEFT JOIN
									(SELECT *
										FROM (SELECT MODELTYPE, SCENARIONAMEUNIQUE, DATE, HOSPITAL_OCCUPANCY, 1 AS PEAK_HOSPITAL_OCCUPANCY
											FROM work.MODEL_FINAL
											GROUP BY 1, 2
											HAVING HOSPITAL_OCCUPANCY=MAX(HOSPITAL_OCCUPANCY)
											) 
										GROUP BY MODELTYPE, SCENARIONAMEUNIQUE
										HAVING DATE=MIN(DATE)
									) HOSP
									ON MF.MODELTYPE = HOSP.MODELTYPE
										AND MF.SCENARIONAMEUNIQUE = HOSP.SCENARIONAMEUNIQUE
										AND MF.DATE = HOSP.DATE
								LEFT JOIN
									(SELECT *
										FROM (SELECT MODELTYPE, SCENARIONAMEUNIQUE, DATE, ICU_OCCUPANCY, 1 AS PEAK_ICU_OCCUPANCY
											FROM work.MODEL_FINAL
											GROUP BY 1, 2
											HAVING ICU_OCCUPANCY=MAX(ICU_OCCUPANCY)
											) 
										GROUP BY MODELTYPE, SCENARIONAMEUNIQUE
										HAVING DATE=MIN(DATE)
									) ICU
									ON MF.MODELTYPE = ICU.MODELTYPE
										AND MF.SCENARIONAMEUNIQUE = ICU.SCENARIONAMEUNIQUE
										AND MF.DATE = ICU.DATE
								LEFT JOIN
									(SELECT *
										FROM (SELECT MODELTYPE, SCENARIONAMEUNIQUE, DATE, VENT_OCCUPANCY, 1 AS PEAK_VENT_OCCUPANCY
											FROM work.MODEL_FINAL
											GROUP BY 1, 2
											HAVING VENT_OCCUPANCY=MAX(VENT_OCCUPANCY)
										) 
										GROUP BY MODELTYPE, SCENARIONAMEUNIQUE
										HAVING DATE=MIN(DATE)
									) VENT
									ON MF.MODELTYPE = VENT.MODELTYPE
										AND MF.SCENARIONAMEUNIQUE = VENT.SCENARIONAMEUNIQUE
										AND MF.DATE = VENT.DATE
								LEFT JOIN
									(SELECT *
										FROM (SELECT MODELTYPE, SCENARIONAMEUNIQUE, DATE, ECMO_OCCUPANCY, 1 AS PEAK_ECMO_OCCUPANCY
											FROM work.MODEL_FINAL
											GROUP BY 1, 2
											HAVING ECMO_OCCUPANCY=MAX(ECMO_OCCUPANCY)
										) 
										GROUP BY MODELTYPE, SCENARIONAMEUNIQUE
										HAVING DATE=MIN(DATE)
									) ECMO
									ON MF.MODELTYPE = ECMO.MODELTYPE
										AND MF.SCENARIONAMEUNIQUE = ECMO.SCENARIONAMEUNIQUE
										AND MF.DATE = ECMO.DATE
								LEFT JOIN
									(SELECT * FROM
										(SELECT MODELTYPE, SCENARIONAMEUNIQUE, DATE, DIAL_OCCUPANCY, 1 AS PEAK_DIAL_OCCUPANCY
											FROM work.MODEL_FINAL
											GROUP BY 1, 2
											HAVING DIAL_OCCUPANCY=MAX(DIAL_OCCUPANCY)
										) 
										GROUP BY MODELTYPE, SCENARIONAMEUNIQUE
										HAVING DATE=MIN(DATE)
									) DIAL
									ON MF.MODELTYPE = DIAL.MODELTYPE
										AND MF.SCENARIONAMEUNIQUE = DIAL.SCENARIONAMEUNIQUE
										AND MF.DATE = DIAL.DATE
								LEFT JOIN
									(SELECT *
										FROM (SELECT MODELTYPE, SCENARIONAMEUNIQUE, DATE, I_N, 1 AS PEAK_I_N
											FROM work.MODEL_FINAL
											GROUP BY 1, 2
											HAVING I_N=MAX(I_N)
										) 
										GROUP BY MODELTYPE, SCENARIONAMEUNIQUE
										HAVING DATE=MIN(DATE)
									) I
									ON MF.MODELTYPE = I.MODELTYPE
										AND MF.SCENARIONAMEUNIQUE = I.SCENARIONAMEUNIQUE
										AND MF.DATE = I.DATE
								LEFT JOIN
									(SELECT *
										FROM (SELECT MODELTYPE, SCENARIONAMEUNIQUE, DATE, FATALITY, 1 AS PEAK_FATALITY
											FROM work.MODEL_FINAL
											GROUP BY 1, 2
											HAVING FATALITY=MAX(FATALITY)
										) 
										GROUP BY MODELTYPE, SCENARIONAMEUNIQUE
										HAVING DATE=MIN(DATE)
									) FATAL
									ON MF.MODELTYPE = FATAL.MODELTYPE
										AND MF.SCENARIONAMEUNIQUE = FATAL.SCENARIONAMEUNIQUE
										AND MF.DATE = FATAL.DATE
							ORDER BY SCENARIONAMEUNIQUE, MODELTYPE, DATE;

							/* add EVENTY columns for ploting labels in ISOChangeEvent */
							select name into :varlist separated by ', '
								from dictionary.columns
								where UPCASE(LIBNAME)="WORK" and upcase(memname)="MODEL_FINAL" and upcase(name) ne 'EVENTY_MULTIPLIER';
							create table work.MODEL_FINAL as
								select * from
									(select &varlist from work.MODEL_FINAL) m1
									left join
									(
										select t1.ScenarioNameUnique, t1.ModelType, t1.Date,
												round(t1.EventY_Multiplier * t2.HOSPITAL_OCCUPANCY,1) as EventY_HOSPITAL_OCCUPANCY,
												round(t1.EventY_Multiplier * t3.ICU_OCCUPANCY,1) as EventY_ICU_OCCUPANCY,
												round(t1.EventY_Multiplier * t4.DIAL_OCCUPANCY,1) as EventY_DIAL_OCCUPANCY,
												round(t1.EventY_Multiplier * t5.ECMO_OCCUPANCY,1) as EventY_ECMO_OCCUPANCY,
												round(t1.EventY_Multiplier * t6.VENT_OCCUPANCY,1) as EventY_VENT_OCCUPANCY
										from
											(select ScenarioNameUnique, ModelType, Date, EventY_Multiplier from work.MODEL_FINAL) t1
											left join
											(select ScenarioNameUnique, ModelType, HOSPITAL_OCCUPANCY from work.Model_FINAL where PEAK_HOSPITAL_OCCUPANCY) t2
											on t1.ScenarioNameUnique=t2.ScenarioNameUnique and t1.ModelType=t2.ModelType
											left join
											(select ScenarioNameUnique, ModelType, ICU_OCCUPANCY from work.Model_FINAL where PEAK_ICU_OCCUPANCY) t3
											on t1.ScenarioNameUnique=t3.ScenarioNameUnique and t1.ModelType=t3.ModelType
											left join
											(select ScenarioNameUnique, ModelType, DIAL_OCCUPANCY from work.Model_FINAL where PEAK_DIAL_OCCUPANCY) t4
											on t1.ScenarioNameUnique=t4.ScenarioNameUnique and t1.ModelType=t4.ModelType
											left join
											(select ScenarioNameUnique, ModelType, ECMO_OCCUPANCY from work.Model_FINAL where PEAK_ECMO_OCCUPANCY) t5
											on t1.ScenarioNameUnique=t5.ScenarioNameUnique and t1.ModelType=t5.ModelType
											left join
											(select ScenarioNameUnique, ModelType, VENT_OCCUPANCY from work.Model_FINAL where PEAK_VENT_OCCUPANCY) t6
											on t1.ScenarioNameUnique=t6.ScenarioNameUnique and t1.ModelType=t6.ModelType
									) m2
								on m1.ScenarioNameUnique=m2.ScenarioNameUnique and m1.ModelType=m2.ModelType and m1.DATE=m2.DATE
							;
					QUIT;
				/* use proc datasets to apply labels to each column of output data table
					except INPUTS which is documented right after the %EasyRun definition
				 */
					PROC DATASETS LIB=WORK NOPRINT;
						MODIFY MODEL_FINAL;
							LABEL
								ADMIT_DATE = "Date of Admission"
								DATE = "Date of Infection"
								DAY = "Day of Pandemic"
								BETA = "Beta Parameter (Contact Rate)"
								RT = "Reproduction Number"
								SocialDistancing = "Social Distancing (% Reduction from Normal)"
								HOSP = "Newly Hospitalized"
								HOSPITAL_OCCUPANCY = "Hospital Census"
								MARKET_HOSP = "Regional Newly Hospitalized"
								MARKET_HOSPITAL_OCCUPANCY = "Regional Hospital Census"
								ICU = "Newly Hospitalized - ICU"
								ICU_OCCUPANCY = "Hospital Census - ICU"
								MARKET_ICU = "Regional Newly Hospitalized - ICU"
								MARKET_ICU_OCCUPANCY = "Regional Hospital Census - ICU"
								MedSurgOccupancy = "Hospital Medical and Surgical Census (non-ICU)"
								Market_MedSurg_Occupancy = "Regional Medical and Surgical Census (non-ICU)"
								VENT = "Newly Hospitalized - Ventilator"
								VENT_OCCUPANCY = "Hospital Census - Ventilator"
								MARKET_VENT = "Regional Newly Hospitalized - Ventilator"
								MARKET_VENT_OCCUPANCY = "Regional Hospital Census - Ventilator"
								DIAL = "Newly Hospitalized - Dialysis"
								DIAL_OCCUPANCY = "Hospital Census - Dialysis"
								MARKET_DIAL = "Regional Newly Hospitalized - Dialysis"
								MARKET_DIAL_OCCUPANCY = "Regional Hospital Census - Dialysis"
								ECMO = "Newly Hospitalized - ECMO"
								ECMO_OCCUPANCY = "Hospital Census - ECMO"
								MARKET_ECMO = "Regional Newly Hospitalized - ECMO"
								MARKET_ECMO_OCCUPANCY = "Regional Hospital Census - ECMO"
								Deceased_Today = "New Hospital Mortality"
								Fatality = "New Hospital Mortality"
								Total_Deaths = "Cumulative Hospital Mortality"
								Market_Deceased_Today = "New Regional Mortality"
								Market_Fatality = "New Regional Mortality"
								Market_Total_Deaths = "Cumulative Regional Mortality"
								N = "Region Population"
								S_N = "Current Susceptible Population"
								E_N = "Current Exposed Population"
								I_N = "Current Infected Population"
								R_N = "Current Recovered Population"
								NEWINFECTED = "Newly Infected Population"
								ModelType = "Model Type Used to Generate Scenario"
								SCALE = "Ratio of Previous Day Population to Current Day Population"
								ScenarioIndex = "Scenario ID: Order"
								ScenarioSource = "Scenario ID: Source (BATCH or UI)"
								ScenarioUser = "Scenario ID: User who created Scenario"
								ScenarioNameUnique = "Unique Scenario ID"
								Scenarioname = "Scenario Name Short"
								LOWER_HOSPITAL_OCCUPANCY="Lower Bound: Hospital Census"
								LOWER_ICU_OCCUPANCY="Lower Bound: Hospital Census - ICU"
								LOWER_VENT_OCCUPANCY="Lower Bound: Hospital Census - Ventilator"
								LOWER_ECMO_OCCUPANCY="Lower Bound: Hospital Census - ECMO"
								LOWER_DIAL_OCCUPANCY="Lower Bound: Hospital Census - Dialysis"
								UPPER_HOSPITAL_OCCUPANCY="Upper Bound: Hospital Census"
								UPPER_ICU_OCCUPANCY="Upper Bound: Hospital Census - ICU"
								UPPER_VENT_OCCUPANCY="Upper Bound: Hospital Census - Ventilator"
								UPPER_ECMO_OCCUPANCY="Upper Bound: Hospital Census - ECMO"
								UPPER_DIAL_OCCUPANCY="Upper Bound: Hospital Census - Dialysis"
								PEAK_HOSPITAL_OCCUPANCY = "Peak Starts: Hospital Census"
								PEAK_ICU_OCCUPANCY = "Peak Starts: Hospital Census - ICU"
								PEAK_VENT_OCCUPANCY = "Peak Starts: Hospital Census - Ventilator"
								PEAK_ECMO_OCCUPANCY = "Peak Starts: Hospital Census - ECMO"
								PEAK_DIAL_OCCUPANCY = "Peak Starts: Hospital Census - Dialysis"
								PEAK_I_N = "Peak Starts: Current Infected Population"
								PEAK_FATALITY = "Peak Starts: New Hospital Mortality"
								ISOChangeEvent = "Event labels for Dates of Change"
								EventY_HOSPITAL_OCCUPANCY = "Y for plotting ISOChangeEvent with HOSPITAL_OCCUPANCY"
								EventY_ICU_OCCUPANCY = "Y for plotting ISOChangeEvent with ICU_OCCUPANCY"
								EventY_VENT_OCCUPANCY = "Y for plotting ISOChangeEvent with VENT_OCCUPANCY"
								EventY_ECMO_OCCUPANCY = "Y for plotting ISOChangeEvent with ECMO_OCCUPANCY"
								EventY_DIAL_OCCUPANCY = "Y for plotting ISOChangeEvent with DIAL_OCCUPANCY"
								;
							MODIFY SCENARIOS;
							LABEL
								scope = "Source Macro for variable"
								name = "Name of the macro variable"
								offset = "Offset for long character macro variables (>200 characters)"
								value = "The value of macro variable name"
								ScenarioIndex = "Scenario ID: Order"
								ScenarioSource = "Scenario ID: Source (BATCH or UI)"
								ScenarioUser = "Scenario ID: User who created Scenario"
								ScenarioNameUnique = "Unique Scenario Name"
								Scenarioname = "Scenario Name Short"
								Stage = "INPUT for input variables - MODEL for all variables"
								;
							MODIFY INPUTS;
							LABEL
								ScenarioIndex = "Scenario ID: Order"
								ScenarioSource = "Scenario ID: Source (BATCH or UI)"
								ScenarioUser = "Scenario ID: User who created Scenario"
								ScenarioNameUnique = "Unique Scenario Name"
								Scenarioname = "Scenario Name Short"
								;

					QUIT;
					RUN;

                %IF &ScenarioSource = BATCH or &ScenarioSource = BOEMSKA %THEN %DO;
                
                    PROC APPEND base=store.MODEL_FINAL data=work.MODEL_FINAL NOWARN FORCE; run;
                    PROC APPEND base=store.SCENARIOS data=work.SCENARIOS; run;
                    PROC APPEND base=store.INPUTS data=work.INPUTS; run;


                    PROC SQL;
                        drop table work.MODEL_FINAL;
                        drop table work.SCENARIOS;
                        drop table work.INPUTS;

                    QUIT;

                %END;

        %END;
        /*%ELSE %IF &PLOTS. = YES %THEN %DO;*/
        %ELSE %DO;
            %IF &ScenarioSource = BATCH or &ScenarioSource = BOEMSKA %THEN %DO;
                PROC SQL; 
                    drop table work.MODEL_FINAL;
                    drop table work.SCENARIOS;
                    drop table work.INPUTS; 
                        %IF &HAVE_SASETS = YES AND %SYMEXIST(ISOChangeDate1) %THEN %DO;
                            drop table work.FIT_PRED;
                            drop table work.FIT_PARMS;
                        %END;
            data work.ds_seir;
                set &PULLLIB..MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SEIR with Data Step"; 
            run;


            data work.ds_sir;
                set &PULLLIB..MODEL_FINAL;
                where  ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SIR with Data Step"; 
            run;

            data work.tmodel_seir;
                set &PULLLIB..MODEL_FINAL;
                where ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SEIR with PROC (T)MODEL"; 
            run;


            data work.tmodel_sir;
                set &PULLLIB..MODEL_FINAL;
                where  ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SIR with PROC (T)MODEL"; 
            run;

            data work.tmodel_seir_fit_i;
                set &PULLLIB..MODEL_FINAL;
                where  ScenarioIndex=&ScenarioIndex_recall. and ScenarioSource="&ScenarioSource_recall." and ScenarioUser="&ScenarioUser_recall." and ModelType = "SEIR with PROC (T)MODEL-Fit R0"; 
            run;
                QUIT;
            %END;
        %END;
%mend;




/* Scenarios can be run in batch by specifying them in a sas dataset.
    In the example below, this dataset is created by reading scenarios from an csv file: run_scenarios.csv
    An example run_scenarios.csv file is provided with this code.

	IMPORTANT NOTES: 
		The example run_scenarios.csv file has columns for all the positional macro variables.  
		There are even more keyword parameters available.
			These need to be set for your population.
			They can be reviewed within the %EasyRun macro at the very top.
		THEN:
			you can set fixed values for the keyword parameters in the %EasyRun definition call
			OR
			you can add columns for the keyword parameters to this input file

	You could also use other files as input sources.  For example, with an excel file you could use libname XLSX.
*/
%macro run_scenarios();


	data WORK.RUN_SCENARIOS;
		set WORK.input_scenarios;
	run;

	/* extract column names into space delimited string stored in macro variable &names */
	PROC SQL noprint;
		select name into :names separated by ' '
	  		from dictionary.columns
	  		where memname = 'RUN_SCENARIOS';
		select name into :dnames separated by ' '
	  		from dictionary.columns
	  		where memname = 'RUN_SCENARIOS' and substr(format,1,4)='DATE';
	QUIT;
	/* change date variables to character and of the form 'ddmmmyyyy'd */
	%IF %SYMEXIST(dnames) %THEN %DO i = 1 %TO %sysfunc(countw(&dnames.));
		%LET dname = %scan(&dnames,&i);
		data run_scenarios(drop=x);
			set run_scenarios(rename=(&dname.=x));
			&dname.="'"||put(x,date9.)||"'d";
		run;
	%END;
	/* build a call to %EasyRun for each row in run_scenarios */
	%GLOBAL cexecute;
	%DO i=1 %TO %sysfunc(countw(&names.));
		%LET next_name = %scan(&names, &i);
		%IF &i = 1 %THEN %DO;
			%LET cexecute = "&next_name.=",&next_name.; 
		%END;
		%ELSE %DO;
			%LET cexecute = &cexecute ,", &next_name.=",&next_name;
		%END;
	%END;
%mend;

%run_scenarios();

/* use the &cexecute variable and the run_scenario dataset to run all the scenarios with call execute */
data _null_;
  set run_scenarios;
  call execute(cats('%nrstr(%EasyRun(',&cexecute.,'));'));
run;

%if %sysfunc(exist(boemska_ds_seir)) %then %do;
	data ds_seir;
		set boemska_ds_seir;
		datetime=dhms(date,0,0,0);
		drop
			ModelType
			ScenarioName
			ScenarioNameUnique
			ScenarioSource
			ScenarioUser
			ScenarioIndex
			;
	run;
%end;

%if %sysfunc(exist(boemska_ds_sir)) %then %do;
	data ds_sir;
		set boemska_ds_sir;
		datetime=dhms(date,0,0,0);
		drop
			ModelType
			ScenarioName
			ScenarioNameUnique
			ScenarioSource
			ScenarioUser
			ScenarioIndex
			;
	run;
%end;


%if %sysfunc(exist(boemska_tmodel_seir)) %then %do;
	data tmodel_seir;
		set boemska_tmodel_seir;
		datetime=dhms(date,0,0,0);
		drop
			ModelType
			ScenarioName
			ScenarioNameUnique
			ScenarioSource
			ScenarioUser
			ScenarioIndex
			;
	run;
%end;

%if %sysfunc(exist(boemska_tmodel_seir_fit_i)) %then %do;
	data tmodel_seir_fit_i;
		set boemska_tmodel_seir_fit_i;
		datetime=dhms(date,0,0,0);
		drop
			ModelType
			ScenarioName
			ScenarioNameUnique
			ScenarioSource
			ScenarioUser
			ScenarioIndex
			;
	run;
%end;

%if %sysfunc(exist(boemska_tmodel_sir)) %then %do;
	data tmodel_sir;
	set boemska_tmodel_sir;
	datetime=dhms(date,0,0,0);
	drop
		ModelType
		ScenarioName
		ScenarioNameUnique
		ScenarioSource
		ScenarioUser
		ScenarioIndex
		;
	run;
%end;


%macro bafCheckoutputs;
* this if sysfunc exist happens for each outtable ;

* --out table 0-- ;
  %if %sysfunc(exist(ds_seir)) = 0 %then %do;
    data ds_seir;
  length DATE $80. ADMIT_DATE $80. S_N 8. I_N 8. E_N 8. R_N 8. DAY 8. NEWINFECTED 8. HOSP 8. ICU 8. VENT 8. ECMO 8. DIAL 8. Fatality 8. Deceased_Today 8. Total_Deaths 8. MARKET_HOSP 8. MARKET_ICU 8. MARKET_VENT 8. MARKET_ECMO 8. MARKET_DIAL 8. Market_Fatality 8. Market_Deceased_Today 8. Market_Total_Deaths 8. HOSPITAL_OCCUPANCY 8. ICU_OCCUPANCY 8. VENT_OCCUPANCY 8. ECMO_OCCUPANCY 8. DIAL_OCCUPANCY 8. MARKET_HOSPITAL_OCCUPANCY 8. MARKET_ICU_OCCUPANCY 8. MARKET_VENT_OCCUPANCY 8. MARKET_ECMO_OCCUPANCY 8. MARKET_DIAL_OCCUPANCY 8. MedSurgOccupancy 8. Market_MEdSurg_Occupancy 8. ISOChangeEvent $80. LOWER_HOSPITAL_OCCUPANCY 8. LOWER_ICU_OCCUPANCY 8. LOWER_VENT_OCCUPANCY 8. LOWER_ECMO_OCCUPANCY 8. LOWER_DIAL_OCCUPANCY 8. UPPER_HOSPITAL_OCCUPANCY 8. UPPER_ICU_OCCUPANCY 8. UPPER_VENT_OCCUPANCY 8. UPPER_ECMO_OCCUPANCY 8. UPPER_DIAL_OCCUPANCY 8. BETA 8. SocialDistancing 8. N 8. SCALE 8. RT 8. EventY_Multiplier 8. datetime 8.;
    run;
  %end;
* --out table 1-- ;
  %if %sysfunc(exist(ds_sir)) = 0 %then %do;
    data ds_sir;
  length DATE $80. ADMIT_DATE $80. S_N 8. I_N 8. E_N 8. R_N 8. DAY 8. NEWINFECTED 8. HOSP 8. ICU 8. VENT 8. ECMO 8. DIAL 8. Fatality 8. Deceased_Today 8. Total_Deaths 8. MARKET_HOSP 8. MARKET_ICU 8. MARKET_VENT 8. MARKET_ECMO 8. MARKET_DIAL 8. Market_Fatality 8. Market_Deceased_Today 8. Market_Total_Deaths 8. HOSPITAL_OCCUPANCY 8. ICU_OCCUPANCY 8. VENT_OCCUPANCY 8. ECMO_OCCUPANCY 8. DIAL_OCCUPANCY 8. MARKET_HOSPITAL_OCCUPANCY 8. MARKET_ICU_OCCUPANCY 8. MARKET_VENT_OCCUPANCY 8. MARKET_ECMO_OCCUPANCY 8. MARKET_DIAL_OCCUPANCY 8. MedSurgOccupancy 8. Market_MEdSurg_Occupancy 8. ISOChangeEvent $80. LOWER_HOSPITAL_OCCUPANCY 8. LOWER_ICU_OCCUPANCY 8. LOWER_VENT_OCCUPANCY 8. LOWER_ECMO_OCCUPANCY 8. LOWER_DIAL_OCCUPANCY 8. UPPER_HOSPITAL_OCCUPANCY 8. UPPER_ICU_OCCUPANCY 8. UPPER_VENT_OCCUPANCY 8. UPPER_ECMO_OCCUPANCY 8. UPPER_DIAL_OCCUPANCY 8. BETA 8. SocialDistancing 8. N 8. SCALE 8. RT 8. EventY_Multiplier 8. datetime 8.;
    run;
  %end;
* --out table 2-- ;
  %if %sysfunc(exist(tmodel_seir)) = 0 %then %do;
    data tmodel_seir;
  length ModelType $80. DATE $80. ADMIT_DATE $80. ScenarioName $80. ScenarioNameUnique $80. ScenarioSource $80. ScenarioUser $80. ScenarioIndex 8. S_N 8. I_N 8. E_N 8. R_N 8. DAY 8. NEWINFECTED 8. HOSP 8. ICU 8. VENT 8. ECMO 8. DIAL 8. Fatality 8. Deceased_Today 8. Total_Deaths 8. MARKET_HOSP 8. MARKET_ICU 8. MARKET_VENT 8. MARKET_ECMO 8. MARKET_DIAL 8. Market_Fatality 8. Market_Deceased_Today 8. Market_Total_Deaths 8. HOSPITAL_OCCUPANCY 8. ICU_OCCUPANCY 8. VENT_OCCUPANCY 8. ECMO_OCCUPANCY 8. DIAL_OCCUPANCY 8. MARKET_HOSPITAL_OCCUPANCY 8. MARKET_ICU_OCCUPANCY 8. MARKET_VENT_OCCUPANCY 8. MARKET_ECMO_OCCUPANCY 8. MARKET_DIAL_OCCUPANCY 8. MedSurgOccupancy 8. Market_MEdSurg_Occupancy 8. ISOChangeEvent $80. LOWER_HOSPITAL_OCCUPANCY 8. LOWER_ICU_OCCUPANCY 8. LOWER_VENT_OCCUPANCY 8. LOWER_ECMO_OCCUPANCY 8. LOWER_DIAL_OCCUPANCY 8. UPPER_HOSPITAL_OCCUPANCY 8. UPPER_ICU_OCCUPANCY 8. UPPER_VENT_OCCUPANCY 8. UPPER_ECMO_OCCUPANCY 8. UPPER_DIAL_OCCUPANCY 8. SocialDistancing 8. BETA 8. RT 8. EventY_Multiplier 8.;
    run;
  %end;
* --out table 3-- ;
  %if %sysfunc(exist(tmodel_seir_fit_i)) = 0 %then %do;
    data tmodel_seir_fit_i;
  length ModelType $80. DATE $80. ADMIT_DATE $80. ScenarioName $80. ScenarioNameUnique $80. ScenarioSource $80. ScenarioUser $80. ScenarioIndex 8. S_N 8. I_N 8. E_N 8. R_N 8. DAY 8. NEWINFECTED 8. HOSP 8. ICU 8. VENT 8. ECMO 8. DIAL 8. Fatality 8. Deceased_Today 8. Total_Deaths 8. MARKET_HOSP 8. MARKET_ICU 8. MARKET_VENT 8. MARKET_ECMO 8. MARKET_DIAL 8. Market_Fatality 8. Market_Deceased_Today 8. Market_Total_Deaths 8. HOSPITAL_OCCUPANCY 8. ICU_OCCUPANCY 8. VENT_OCCUPANCY 8. ECMO_OCCUPANCY 8. DIAL_OCCUPANCY 8. MARKET_HOSPITAL_OCCUPANCY 8. MARKET_ICU_OCCUPANCY 8. MARKET_VENT_OCCUPANCY 8. MARKET_ECMO_OCCUPANCY 8. MARKET_DIAL_OCCUPANCY 8. MedSurgOccupancy 8. Market_MEdSurg_Occupancy 8. ISOChangeEvent $80. LOWER_HOSPITAL_OCCUPANCY 8. LOWER_ICU_OCCUPANCY 8. LOWER_VENT_OCCUPANCY 8. LOWER_ECMO_OCCUPANCY 8. LOWER_DIAL_OCCUPANCY 8. UPPER_HOSPITAL_OCCUPANCY 8. UPPER_ICU_OCCUPANCY 8. UPPER_VENT_OCCUPANCY 8. UPPER_ECMO_OCCUPANCY 8. UPPER_DIAL_OCCUPANCY 8. SocialDistancing 8. BETA 8. RT 8. EventY_Multiplier 8.;
    run;
  %end;
* --out table 4-- ;
  %if %sysfunc(exist(tmodel_sir)) = 0 %then %do;
    data tmodel_sir;
  length DATE $80. ADMIT_DATE $80. S_N 8. I_N 8. E_N 8. R_N 8. DAY 8. NEWINFECTED 8. HOSP 8. ICU 8. VENT 8. ECMO 8. DIAL 8. Fatality 8. Deceased_Today 8. Total_Deaths 8. MARKET_HOSP 8. MARKET_ICU 8. MARKET_VENT 8. MARKET_ECMO 8. MARKET_DIAL 8. Market_Fatality 8. Market_Deceased_Today 8. Market_Total_Deaths 8. HOSPITAL_OCCUPANCY 8. ICU_OCCUPANCY 8. VENT_OCCUPANCY 8. ECMO_OCCUPANCY 8. DIAL_OCCUPANCY 8. MARKET_HOSPITAL_OCCUPANCY 8. MARKET_ICU_OCCUPANCY 8. MARKET_VENT_OCCUPANCY 8. MARKET_ECMO_OCCUPANCY 8. MARKET_DIAL_OCCUPANCY 8. MedSurgOccupancy 8. Market_MEdSurg_Occupancy 8. ISOChangeEvent $80. LOWER_HOSPITAL_OCCUPANCY 8. LOWER_ICU_OCCUPANCY 8. LOWER_VENT_OCCUPANCY 8. LOWER_ECMO_OCCUPANCY 8. LOWER_DIAL_OCCUPANCY 8. UPPER_HOSPITAL_OCCUPANCY 8. UPPER_ICU_OCCUPANCY 8. UPPER_VENT_OCCUPANCY 8. UPPER_ECMO_OCCUPANCY 8. UPPER_DIAL_OCCUPANCY 8. SocialDistancing 8. BETA 8. RT 8. EventY_Multiplier 8.;
    run;
  %end;
%mend; %bafCheckoutputs;
%bafheader;
    %bafOutDataset(ds_seir, work, ds_seir, h54skeys=nokeys);
    %bafOutDataset(ds_sir, work, ds_sir, h54skeys=nokeys);
    %bafOutDataset(tmodel_seir, work, tmodel_seir, h54skeys=nokeys);
    %bafOutDataset(tmodel_seir_fit_i, work, tmodel_seir_fit_i, h54skeys=nokeys);
    %bafOutDataset(tmodel_sir, work, tmodel_sir, h54skeys=nokeys);
%bafFooter;