
%macro xocalcs;

	%macro initiatevars(type);

		/*Sold*/
		SoldNum_&type=0;

		/*Total*/
		TotalNum_&type=0;								TotalOrigBal_&type=0;					TotalCurrBal_&type=0;
		OldestAcc_&type=.;								NewestAcc_&type=.;
		MaxPayDate_&type=.;

		/*Paying*/
		PayingNum_&type=0;							PayingOrigBal_&type=0;					PayingCurrBal_&type=0;
		PayingIntNum_&type=0;						PayingIntOrigBal_&type=0;			PayingIntCurrBal_&type=0;
		PayingExtNum_&type=0;						PayingExtOrigBal_&type=0;			PayingExtCurrBal_&type=0;
		PayingFirstPayDate_&type=.;
		PayingMaxStartDate_&type=.;

		/*Settled*/
		SettledNum_&type=0;								SettledOrigBal_&type=0;						SettledAmount_&type=0;
		SettledSetupNum_&type=0;					SettledSetupOrigBal_&type=0;
		SettledSettlementNum_&type=0;			SettledSettlementOrigBal_&type=0;
		SettledDiscountNum_&type=0;				SettledDiscountAmount_&type=0;

		SettledInternalNum_&type=0;				SettledInternalOrigBal_&type=0;
		SettledExternalNum_&type=0;				SettledExternalOrigBal_&type=0;
		SettledDirectNum_&type=0;					SettledDirectOrigBal_&type=0;

		SettledNumL3M_&type=0;						SettledOrigBalL3M_&type=0;				SettledAmountL3M_&type=0;
		SettledNumL6M_&type=0;						SettledOrigBalL6M_&type=0;				SettledAmountL6M_&type=0;
		SettledNumL12M_&type=0;						SettledOrigBalL12M_&type=0;				SettledAmountL12M_&type=0;
		SettledNumL24M_&type=0;						SettledOrigBalL24M_&type=0;				SettledAmountL24M_&type=0;

		FirstSettledAcc_&type=.;						LastSettledAcc_&type=.;

		/*Paid*/
		PaidNum_&type=0;									PaidOrigBal_&type=0;							PaidCurrBal_&type=0;
		PaidAmount_&type=0;
		PaidMaxPayDate_&type=.;						PaidMinPayDate_&type=.;

		/*Non-paid*/
		NonPaidNum_&type=0;									NonPaidBal_&type=0;
		NonPaidChaseNum_&type=0;					NonPaidChaseBal_&type=0;
		NonPaidDCANum_&type=0;							NonPaidDCABal_&type=0;
		NonPaidPreOutNum_&type=0;					NonPaidPreOutBal_&type=0;
		NonPaidActExhNum_&type=0;					NonPaidActExhBal_&type=0;
		NonPaidCustServNum_&type=0;					NonPaidCustServBal_&type=0;
		NonPaidNegNum_&type=0;							NonPaidNegBal_&type=0;

		NonPaidMinStart_&type=.;						NonPaidMaxStart_&type=.;	
		
		/*General*/
		AmountPaidL1M_&type=0;							AmountPaidL3M_&type=0;								AmountPaidL6M_&type=0;									AmountPaidL12M_&type=0;
		TotalAmountPaid_&type=0;
		NumPayments_&type=0;

		NumDeceased_&type=0;							
		NumInsolvency_&type=0;
		NumVulnerable_&type=0;
		NumDMP_&type=0;
		NumOver70_&type=0;

		LitigationNum_&type=0;					LitigationPayments_&type=0;
		LitigationOrigBal_&type=0;				LitigationCurrBal_&type=0;
		MaxLitigation_&type=.;					MinLitigation_&type=.;

		NumSTB_&type=0;	
		OrigBalSTB_&type=0;
		CurrBalSTB_&type=0;

		BWAccs_&type=0;
		Lit_PayingNum_&type=0;					Lit_SensitiveNum_&type=0;					Lit_DMNum_&type=0;
		Lit_DMPNum_&type=0;						Lit_DeceasedNum_&type=0;
		Lit_PrevSelected_&type=0;				Lit_InsolvencyNum_&type=0;

		MaxLitSelectDate_&type=.;				MaxLitActivityDate_&type=.;					MaxPaymentDateLit_&type=.;
		MaxLitClaimDate_&type=.;
	%mend;

	%macro initiatecontact;

		NumRPCs=0;
		NumInboundRPCs=0;						NumOutboundRPCs=0;
		NumInboundRPCsL3M=0;				NumInboundRPCsL6M=0;			NumInboundRPCsL12M=0;
		NumOutboundRPCsL3M=0;			NumOutboundRPCsL6M=0;			NumOutboundRPCsL12M=0;

		MaxRPC=.;
		MaxInRPC=.;			MaxOutRPC=.;
		MinRPC=.;
		MinInRPC=.;			MinOutRPC=.;

		Affordability=0;
		Afford_Income=-1;
		Afford_Expenditure=-1;
		Afford_DisposableIncome=-1;
		Afford_Benefits=-1;
		Afford_RentMortgage=-1;
		Afford_Utilities=-1;
		Afford_LoanCC=-1;
		Afford_Food=-1;
		Afford_NetSalary=-1;
		Afford_Insurance=-1;

		LAS=0;
		NA=0;
		NEG=0;

	%mend;

	%macro calculations(type);

		if linked_statuscode=:'918' or linked_statuscode='910' then do;
			SoldNum_&type+1;
		end;
		else do;

			/*Total*/
			TotalNum_&type+1;
			TotalOrigBal_&type+linked_origbalance;
			TotalCurrBal_&type+linked_currbalance;

			OldestAcc_&type=smallest(1,OldestAcc_&type,linked_startdate);
			NewestAcc_&type=max(NewestAcc_&type,linked_startdate);
			MaxPayDate_&type=max(MaxPayDate_&type,linked_lastpaydate);

			/*Paying*/
			if substr(linked_statuscode,1,1) in ('2','3','5','6','8') and linked_lastpaydate>intnx('dtday',startdate,-35,'s') then do;
				PayingNum_&type+1;
				PayingOrigBal_&type+linked_origbalance;
				PayingCurrBal_&type+linked_currbalance;

				if linked_internalpaydate=linked_lastpaydate then do;
					PayingIntNum_&type+1;
					PayingIntOrigBal_&type+linked_origbalance;
					PayingIntCurrBal_&type+linked_currbalance;
				end;
				else if linked_externalpaydate=linked_lastpaydate then do;
					PayingExtNum_&type+1;
					PayingExtOrigBal_&type+linked_origbalance;
					PayingExtCurrBal_&type+linked_currbalance;
				end;

				PayingFirstPayDate_&type=smallest(1,PayingFirstPayDate_&type,linked_minpaydate);
				PayingMaxStartDate_&type=max(PayingMaxStartDate_&type,linked_startdate);
			end;

			/*Settled*/
			if linked_statuscode='901' then do;
				SettledNum_&type+1;
				SettledOrigBal_&type+linked_origbalance;
				SettledAmount_&type+linked_totalpay;

				if linked_countpay<=3 then do;
					SettledSettlementNum_&type+1;
					SettledSettlementOrigBal_&type+linked_origbalance;
				end;
				else do;
					SettledSetupNum_&type+1;
					SettledSetupOrigBal_&type+linked_origbalance;
				end;

				if linked_totalpay<linked_origbalance then do;
					SettledDiscountNum_&type+1;
					SettledDiscountAmount_&type+(linked_origbalance-linked_totalpay);
				end;

				if linked_internalpaydate=linked_lastpaydate then do;
					SettledInternalNum_&type+1;
					SettledInternalOrigBal_&type+linked_origbalance;
				end;
				if linked_externalpaydate=linked_lastpaydate then do;
					SettledExternalNum_&type+1;
					SettledExternalOrigBal_&type+linked_origbalance;
				end;
				if linked_directpaydate=linked_lastpaydate then do;
					SettledDirectNum_&type+1;
					SettledDirectOrigBal_&type+linked_origbalance;
				end;

				if linked_lastpaydate>intnx('dtmonth',startdate,-3,'s') then do;
					SettledNumL3M_&type+1;
					SettledOrigBalL3M_&type+linked_origbalance;
					SettledAmountL3M_&type+linked_totalpay;
				end;
				if linked_lastpaydate>intnx('dtmonth',startdate,-6,'s') then do;
					SettledNumL6M_&type+1;
					SettledOrigBalL6M_&type+linked_origbalance;
					SettledAmountL6M_&type+linked_totalpay;
				end;
				if linked_lastpaydate>intnx('dtmonth',startdate,-12,'s') then do;
					SettledNumL12M_&type+1;
					SettledOrigBalL12M_&type+linked_origbalance;
					SettledAmountL12M_&type+linked_totalpay;
				end;
				if linked_lastpaydate>intnx('dtmonth',startdate,-24,'s') then do;
					SettledNumL24M_&type+1;
					SettledOrigBalL24M_&type+linked_origbalance;
					SettledAmountL24M_&type+linked_totalpay;
				end;

				FirstSettledAcc_&type=smallest(1,FirstSettledAcc_&type,linked_minpaydate);
				LastSettledAcc_&type=largest(1,LastSettledAcc_&type,linked_lastpaydate);
			end;

			/*Paid*/
			if /*intnx('dtday',startdate,-730,'s')<*/ 0<linked_lastpaydate<=intnx('dtday',startdate,-35,'s') and linked_statuscode ne '901' and linked_totalpay>0 then do;
				PaidNum_&type+1;
				PaidOrigBal_&type+linked_origbalance;
				PaidCurrBal_&type+linked_currbalance;
				PaidAmount_&type+(linked_origbalance-linked_currbalance);

				PaidMinPayDate_&type=smallest(1,PaidMinPayDate_&type,linked_minpaydate);
				PaidMaxPayDate_&type=largest(1,PaidMaxPayDate_&type,linked_lastpaydate);
			
			end;

			/*Non-paid*/
			if linked_totalpay<=0 then do;
				NonPaidNum_&type+1;
				NonPaidBal_&type+linked_origbalance;

				NonPaidMinStart_&type=smallest(1,NonPaidMinStart_&type,linked_startdate);
				NonPaidMaxStart_&type=largest(1,NonPaidMaxStart_&type,linked_startdate);

				if substr(linked_statuscode,1,1) in ('2','3') then do;
					NonPaidChaseNum_&type+1;
					NonPaidChaseBal_&type+linked_origbalance;
				end;
				else if linked_statuscode='899' or (linked_statuscode=:'8' and linked_statuscode^=:'816' and linked_statuscode^=:'850' and linked_statuscode^=:'86') then do;
					NonPaidDCANum_&type+1;
					NonPaidDCABal_&type+linked_origbalance;
				end;
				else if linked_statuscode=:'891' or linked_statuscode=:'816' then do;
					NonPaidPreOutNum_&type+1;
					NonPaidPreOutBal_&type+linked_origbalance;
				end;
				else if linked_statuscode='850H' then do;
					NonPaidActExhNum_&type+1;
					NonPaidActExhBal_&type+linked_origbalance;
				end;
				else if linked_statuscode=:'4' then do;
					NonPaidCustServNum_&type+1;
					NonPaidCustServBal_&type+linked_origbalance;
				end;
				else if linked_statuscode=:'1' then do;
					NonPaidNegNum_&type+1;
					NonPaidNegBal_&type+linked_origbalance;
				end;
			end;

			/*General*/
			AmountPaidL1M_&type+linked_totalpayL1M;					
			AmountPaidL3M_&type+linked_totalpayL3M;
			AmountPaidL6M_&type+linked_totalpayL6M;
			AmountPaidL12M_&type+linked_totalpayL12M;

			TotalAmountPaid_&type+linked_totalpay;
			NumPayments_&type+linked_numpayments;

			if linked_statuscode in ('902','433','816D','891A') then NumDeceased_&type+1;
/*			if linked_statuscode in ('918G','918I','492O') or linked_statuscode=:'497' or linked_statuscode=:'903' then NumInsolvency_&type+1;*/
			if linked_statuscode=:'492' or linked_statuscode=:'495' or linked_statuscode=:'496' or linked_statuscode=:'497' then NumInsolvency_&type+1;
			if linked_statuscode=:'468' or linked_strategycode=:'DMPX' or linked_statuscode='926' then NumVulnerable_&type+1;
			if linked_strategycode=:'DMP' and linked_strategycode^=:'DMPX' and linked_strategycode ne 'DMPNOD' and linked_statuscode^=:'9' then NumDMP_&type+1;
			if linked_DOB<intnx('dtyear',startdate,-70,'s') then NumOver70_&type+1;

			if linked_litigationdate>0 then do;
				LitigationNum_&type+1;
				LitigationPayments_&type+linked_litigationpayments;
				LitigationOrigBal_&type+linked_origbalance;
				LitigationCurrBal_&type+linked_currbalance;
				MaxLitigation_&type=largest(1,MaxLitigation_&type,linked_litigationdate);
				MinLitigation_&type=smallest(1,MinLitigation_&type,linked_litigationdate);
			end;

			if linked_StatuteBarredFlag='StatuteBarred' then do;
				NumSTB_&type+1;
				OrigBalSTB_&type+linked_origbalance;
				CurrBalSTB_&type+linked_currbalance;
			end;

			if linked_statuscode=:'836' then BWAccs_&type+1;
/*			if linked_statuscode=:'5' or (linked_lastpaydate>intnx('dtday',startdate,-60,'s') and linked_totalpay>0) then Lit_PayingNum_&type+1;*/
			if linked_statuscode=:'5' 
				or linked_internalpaydate>intnx('dtday',startdate,-61,'s')
				or linked_activeplan=1
				then Lit_PayingNum_&type+1;
/*			if linked_statuscode=:'469' then Lit_DMPNum_&type+1;*/
			if linked_statuscode=:'469' 
				or linked_strategycode=:'DMC%'
				or linked_strategycode=:'DMP%'
				then Lit_DMPNum_&type+1;
			if linked_prevselected=1 then Lit_PrevSelected_&type+1;

			if linked_statuscode='926' or linked_statuscategory='Sensitive' then Lit_SensitiveNum_&type+1;
			if index(linked_statusdescription,'Deceased') then Lit_DeceasedNum_&type+1;
			if linked_statuscategory='Insolvency' then Lit_InsolvencyNum_&type+1;
			if linked_statuscategory='DM' then Lit_DMNum_&type+1;

			if linked_LitSelectionDate>MaxLitSelectDate_&type then MaxLitSelectDate_&type=linked_LitSelectionDate;
			if linked_LitActivityDate>MaxLitActivityDate_&type then MaxLitActivityDate_&type=linked_LitActivityDate;
			if linked_LitClaimDate>MaxLitClaimDate_&type then MaxLitClaimDate_&type=linked_LitClaimDate;
			if linked_paydatelit>MaxPaymentDateLit_&type then MaxPaymentDateLit_&type=linked_paydatelit;

		end;
			
	%mend;

	%macro contactcalculations;

		NumRPCs+linked_rpcs;
		NumInboundRPCs+linked_inrpcs;
		NumOutboundRPCs+linked_outrpcs;

		NumInboundRPCsL3M+linked_outrpcsl3m;
		NumInboundRPCsL6M+linked_outrpcsl6m;
		NumInboundRPCsL12M+linked_outrpcsl12m;

		NumOutboundRPCsL3M+linked_outrpcsl3m;
		NumOutboundRPCsL6M+linked_outrpcsl6m;
		NumOutboundRPCsL12M+linked_outrpcsl12m;

		MaxRPC=largest(1,MaxRPC,linked_maxrpc);
		MaxInRPC=largest(1,MaxInRPC,linked_maxinrpc);
		MaxOutRPC=largest(1,MaxOutRPC,linked_maxoutrpc);

		MinRPC=smallest(1,MinRPC,linked_Minrpc);
		MinInRPC=smallest(1,MinInRPC,linked_Mininrpc);
		MinOutRPC=smallest(1,MinOutRPC,linked_Minoutrpc);

/*		if affordability=0 and missing(linked_lastaffordability)=0 then do;*/
/*			affordability=1;*/
/*			Afford_Income=linked_totalincome;*/
/*			Afford_Expenditure=linked_totalexpenditure;*/
/*			Afford_DisposableIncome=linked_disposableincome;*/
/*			Afford_Benefits=linked_benefits;*/
/*			Afford_RentMortgage=linked_rentmortgage;*/
/*			Afford_Utilities=linked_utilities;*/
/*			Afford_LoanCC=linked_loanscreditcards;*/
/*			Afford_Food=linked_food;*/
/*			Afford_NetSalary=linked_netsalary;*/
/*			Afford_Insurance=linked_insurance;*/
/*		end;*/

		if linked_statuscode=:'1' then Neg+1;
		else 
		if linked_statuscode^=:'9' then 
		do;
			if compress(upcase(postcode))=compress(upcase(linked_postcode)) then LAS+1;
			else if missing(linked_postcode)=0 then NA+1;
		end;
		else 
		if linked_statuscode=:'9' and linked_lastpaydate>=intnx('dtmonth',startdate,-6,'s') then 
		do;
			if compress(upcase(postcode))=compress(upcase(linked_postcode)) then LAS+1;
			else if missing(linked_postcode)=0 then NA+1;
		end;
		else 
		if linked_statuscode=:'9' and linked_lastpaydate<intnx('dtmonth',startdate,-6,'s') then 
		do;
			if compress(upcase(postcode))=compress(upcase(linked_postcode)) then LAS+1;
			else if missing(linked_postcode)=0 then NA+1;
		end;


	%mend;


	%macro summary(type);
		
		/*Total*/
		if missing(OldestAcc_&type)=0 then AgeOldestCustAcc_&type=intck('dtmonth',OldestAcc_&type,startdate);																else AgeOldestCustAcc_&type=-99999;
		if missing(NewestAcc_&type)=0 then AgeNewestAcc_&type=intck('dtmonth',NewestAcc_&type,startdate);																	else AgeNewestAcc_&type=-99999;
		if TotalNum_&type>0 then AvgCustBal_&type=TotalOrigBal_&type/TotalNum_&type;																										else AvgCustBal_&type=-99999;

		if missing(OldestAcc_&type)=0 then MthsBetweenOldNewAcc_&type=intck('dtmonth',OldestAcc_&type,NewestAcc_&type);									else MthsBetweenOldNewAcc_&type=-99999;
		if missing(MaxPayDate_&type)=0 then MthsSinceLastclient_namePay_&type=intck('dtmonth',MaxPayDate_&type,startdate);											else MthsSinceLastclient_namePay_&type=-99999;

		/*Paying*/
		if missing(PayingFirstPayDate_&type)=0 then MonthSincePayingFirstPay_&type=intck('dtmonth',PayingFirstPayDate_&type,startdate);			else MonthSincePayingFirstPay_&type=-99999;
		if TotalNum_&type>0 then PctAccsPaying_&type=PayingNum_&type/TotalNum_&type;																									else PctAccsPaying_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalancePaying_&type=PayingOrigBal_&type/TotalOrigBal_&type;																				else PctBalancePaying_&type=-99999;
		if PayingOrigBal_&type>0 then SaleBalPctPayingBal_&type=balance/PayingOrigBal_&type;																						else SaleBalPctPayingBal_&type=-99999;
		if PayingNum_&type>0 then PctPayingAccsInt_&type=PayingIntNum_&type/PayingNum_&type;																					else PctPayingAccsInt_&type=-99999;
		if PayingNum_&type>0 then AvgBalPaying_&type=PayingOrigBal_&type/PayingNum_&type;																						else AvgBalPaying_&type=-99999;


		/*Settled*/
		if SettledOrigBal_&type>0 then SettAmtPctOrigBal_&type=SettledAmount_&type/SettledOrigBal_&type;																		else SettAmtPctOrigBal_&type=-99999;
		if TotalNum_&type>0 then PctAccsSettled_&type=SettledNum_&type/TotalNum_&type;																										else PctAccsSettled_&type=-99999;
		if TotalOrigBal_&type then PctBalanceSettled_&type=SettledOrigBal_&type/TotalOrigBal_&type;																					else PctBalanceSettled_&type=-99999;
		if SettledNum_&type>0 then AvgSettledBal_&type=SettledOrigBal_&type/SettledNum_&type;																							else AvgSettledBal_&type=-99999;
		if SettledNum_&type>0 then AvgSettledAmount_&type=SettledAmount_&type/SettledNum_&type;																					else AvgSettledAmount_&type=-99999;

		if missing(FirstSettledAcc_&type)=0 then AgeOldestSettAcc_&type=intck('dtmonth',FirstSettledAcc_&type,startdate);												else AgeOldestSettAcc_&type=-99999;
		if missing(LastSettledAcc_&type)=0 then AgeNewestSettAcc_&type=intck('dtmonth',LastSettledAcc_&type,startdate);												else AgeNewestSettAcc_&type=-99999;
		if missing(FirstSettledAcc_&type)=0 then MthsBetweenOldNewSett_&type=intck('dtmonth',FirstSettledAcc_&type,LastSettledAcc_&type);			else MthsBetweenOldNewSett_&type=-99999;	

		/*Paid*/
		if TotalNum_&type>0 then PctAccsPaid_&type=PaidNum_&type/TotalNum_&type;																												else PctAccsPaid_&type=-99999;
		if TotalOrigBal_&type>0 then PaidPctBalPaid_&type=PaidOrigBal_&type/TotalOrigBal_&type;																							else PaidPctBalPaid_&type=-99999;
		if PaidOrigBal_&type>0 then PaidPctBal_&type=1-PaidCurrBal_&type/PaidOrigBal_&type;																								else PaidPctBal_&type=-99999;

		if missing(PaidMinPayDate_&type)=0 then PaidMthsFirstPay_&type=intck('dtmonth',PaidMinPayDate_&type,startdate);												else PaidMthsFirstPay_&type=-99999;
		if missing(PaidMaxPayDate_&type)=0 then PaidMthsLastPay_&type=intck('dtmonth',PaidMaxPayDate_&type,startdate);											else PaidMthsLastPay_&type=-99999;
		
		/*Non-paid*/
		if TotalNum_&type>0 then PctAccsNonPaid_&type=NonPaidNum_&type/TotalNum_&type;																									else PctAccsNonPaid_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalNonPaid_&type=NonPaidBal_&type/TotalOrigBal_&type;																							else PctBalNonPaid_&type=-99999;
		if TotalNum_&type>0 then PctAccsNonPaidChase_&type=NonPaidChaseNum_&type/TotalNum_&type;																			else PctAccsNonPaidChase_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalNonPaidChase_&type=NonPaidChaseBal_&type/TotalOrigBal_&type;																		else PctBalNonPaidChase_&type=-99999;
		if TotalNum_&type>0 then PctAccsNonPaidDCA_&type=NonPaidDCANum_&type/TotalNum_&type;																					else PctAccsNonPaidDCA_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalNonPaidDCA_&type=NonPaidDCABal_&type/TotalOrigBal_&type;																				else PctBalNonPaidDCA_&type=-99999;
		if TotalNum_&type>0 then PctAccsNonPaidPreOut_&type=NonPaidPreOutNum_&type/TotalNum_&type;																			else PctAccsNonPaidPreOut_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalNonPaidPreOut_&type=NonPaidPreOutBal_&type/TotalOrigBal_&type;																	else PctBalNonPaidPreOut_&type=-99999;
		if TotalNum_&type>0 then PctAccsNonPaidActExh_&type=NonPaidActExhNum_&type/TotalNum_&type;																			else PctAccsNonPaidActExh_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalNonPaidActExh_&type=NonPaidActExhBal_&type/TotalOrigBal_&type;																	else PctBalNonPaidActExh_&type=-99999;
		if TotalNum_&type>0 then PctAccsNonPaidCustServ_&type=NonPaidCustServNum_&type/TotalNum_&type;																	else PctAccsNonPaidCustServ_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalNonPaidCustServ_&type=NonPaidCustServBal_&type/TotalOrigBal_&type;															else PctBalNonPaidCustServ_&type=-99999;
		if TotalNum_&type>0 then PctAccsNonPaidNeg_&type=NonPaidNegNum_&type/TotalNum_&type;																					else PctAccsNonPaidNeg_&type=-99999;
		if TotalOrigBal_&type>0 then PctBalNonPaidNeg_&type=NonPaidNegBal_&type/TotalOrigBal_&type;																				else PctBalNonPaidNeg_&type=-99999;

		if missing(NonPaidMinStart_&type)=0 then AgeOldestNonPaid_&type=intck('dtmonth',NonPaidMinStart_&type,startdate);										else AgeOldestNonPaid_&type=-99999;
		if missing(NonPaidMaxStart_&type)=0 then AgeNewestNonPaid_&type=intck('dtmonth',NonPaidMaxStart_&type,startdate);										else AgeNewestNonPaid_&type=-99999;
		if missing(NonPaidMinStart_&type)=0 then MthsBetweenOldNewNonPaid_&type=intck('dtmonth',NonPaidMinStart_&type,NonPaidMaxStart_&type);	else MthsBetweenOldNewNonPaid_&type=-99999;

		/*General*/
		if sum(TotalCurrBal_&type,AmountPaidL1M_&type)>0 then PctPaidL1M_&type=AmountPaidL1M_&type/sum(TotalCurrBal_&type,AmountPaidL1M_&type);					else PctPaidL1M_&type=-99999;
		if sum(TotalCurrBal_&type,AmountPaidL3M_&type)>0 then PctPaidL3M_&type=AmountPaidL3M_&type/sum(TotalCurrBal_&type,AmountPaidL3M_&type);					else PctPaidL3M_&type=-99999;
		if sum(TotalCurrBal_&type,AmountPaidL6M_&type)>0 then PctPaidL6M_&type=AmountPaidL6M_&type/sum(TotalCurrBal_&type,AmountPaidL6M_&type);					else PctPaidL6M_&type=-99999;
		if sum(TotalCurrBal_&type,AmountPaidL12M_&type)>0 then PctPaidL12M_&type=AmountPaidL12M_&type/sum(TotalCurrBal_&type,AmountPaidL12M_&type);			else PctPaidL12M_&type=-99999;

		if NumPayments_&type>0 then AvgPayment_&type=TotalAmountPaid_&type/NumPayments_&type;																					else AvgPayment_&type=-99999;

		if TotalNum_&type>0 then PctAccsDeceased_&type=NumDeceased_&type/TotalNum_&type;																								else PctAccsDeceased_&type=-99999;
		if TotalNum_&type>0 then PctAccsInsolvency_&type=NumInsolvency_&type/TotalNum_&type;																							else PctAccsInsolvency_&type=-99999;
		if TotalNum_&type>0 then PctAccsVulnerable_&type=NumVulnerable_&type/TotalNum_&type;																							else PctAccsVulnerable_&type=-99999;
		if TotalNum_&type>0 then PctAccsDMP_&type=NumDMP_&type/TotalNum_&type;																													else PctAccsDMP_&type=-99999;
		if TotalNum_&type>0 then PctAccsOver70_&type=NumOver70_&type/TotalNum_&type;																										else PctAccsOver70_&type=-99999;

		if TotalNum_&type>0 then PctAccsLitigation_&type=LitigationNum_&type/TotalNum_&type;																								else PctAccsLitigation_&type=-99999;
		if TotalCurrBal_&type>0 then PctBalLitigation_&type=LitigationCurrBal_&type/TotalCurrBal_&type;																					else PctBalLitigation_&type=-99999;
		if sum(LitigationCurrBal_&type,LitigationPayments_&type)>0 then LitigationCollsPct_&type=LitigationPayments_&type/sum(LitigationCurrBal_&type,LitigationPayments_&type);
																																																																			else LitigationCollsPct_&type=-99999;
		if missing(MaxLitigation_&type)=0 then MthsSinceLastLitigation_&type=intck('dtmonth',MaxLitigation_&type,startdate);											else MthsSinceLastLitigation_&type=-99999;
		if missing(MinLitigation_&type)=0 then MthsSinceFirstLitigation_&type=intck('dtmonth',MinLitigation_&type,startdate);											else MthsSinceFirstLitigation_&type=-99999;
		
		/*Sale file charateristics*/
		if TotalOrigBal_&type>0 then SaleBalPctOrigBal_&type=balance/TotalOrigBal_&type;																											else SaleBalPctOrigBal_&type=-99999;
		if TotalCurrBal_&type>0 then SaleBalPctCurrBal_&type=balance/TotalCurrBal_&type;																											else SaleBalPctCurrBal_&type=-99999;
		if PayingOrigBal_&type>0 then SaleBalPctPayOrigBal_&type=balance/PayingOrigBal_&type;																								else SaleBalPctPayOrigBal_&type=-99999;
		if PayingCurrBal_&type>0 then SaleBalPctPayCurrBal_&type=balance/PayingCurrBal_&type;																								else SaleBalPctPayCurrBal_&type=-99999;
		if SettledOrigBal_&type>0 then SaleBalPctSettBal_&type=balance/SettledOrigBal_&type;																										else SaleBalPctSettBal_&type=-99999;

		if NumSTB_&type>0 then PctAccsSTB_&type=TotalNum_&type/NumSTB_&type;																															else PctAccsSTB_&type=-99999;
		if CurrBalSTB_&type>0 then PctCurrBalSTB_&type=TotalCurrBal_&type/CurrBalSTB_&type;																										else PctCurrBalSTB_&type=-99999;

		drop 	
			OldestAcc_&type NewestAcc_&type 
			PayingFirstPayDate_&type PayingMaxStartDate_&type
			FirstSettledAcc_&type LastSettledAcc_&type
			PaidMinPayDate_&type PaidMaxPayDate_&type
			NonPaidMinStart_&type NonPaidMaxStart_&type
			MaxPayDate_&type
		;

	%mend;

	%macro contactsummary;

		if missing(MinRPC)=0 then MthsSinceFirstRPC=intck('dtmonth',MinRPC,startdate);															else MthsSinceFirstRPC=-99999;
		if missing(MaxRPC)=0 then MthsSinceLastRPC=intck('dtmonth',MaxRPC,startdate);															else MthsSinceLastRPC=-99999;

		if missing(MinInRPC)=0 then MthsSinceFirstInRPC=intck('dtmonth',MinInRPC,startdate);												else MthsSinceFirstInRPC=-99999;
		if missing(MaxInRPC)=0 then MthsSinceLastInRPC=intck('dtmonth',MaxInRPC,startdate);												else MthsSinceLastInRPC=-99999;

		if missing(MinOutRPC)=0 then MthsSinceFirstOutRPC=intck('dtmonth',MinOutRPC,startdate);										else MthsSinceFirstOutRPC=-99999;
		if missing(MaxOutRPC)=0 then MthsSinceLastOutRPC=intck('dtmonth',MaxOutRPC,startdate);										else MthsSinceLastOutRPC=-99999;

		if missing(MinRPC)=0 then MthsBetweenFirstLastRPC=intck('dtmonth',MinRPC,MaxRPC);													else MthsBetweenFirstLastRPC=-99999;
		if missing(MinInRPC)=0 then MthsBetweenFirstLastInRPC=intck('dtmonth',MinInRPC,MaxInRPC);									else MthsBetweenFirstLastInRPC=-99999;
		if missing(MinOutRPC)=0 then MthsBetweenFirstLastOutRPC=intck('dtmonth',MinOutRPC,MaxOutRPC);					else MthsBetweenFirstLastOutRPC=-99999;

		if na>0 then TraceTypeNew='NA';
		else if las>0 then TraceTypeNew='LAS';
		else TraceTypeNew='Neg';

		drop
			MinRPC MaxRPC
			MinInRPC MaxInRPC
			MinOutRPC MaxOutRPC
			las na neg
		;

	%mend;

%mend;