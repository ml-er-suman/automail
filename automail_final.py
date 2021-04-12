#!/usr/bin/env python
# coding: utf-8

# In[20]:

import re
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import os
from itertools import chain
import errno

pd.set_option("display.max_columns",100)
pd.options.display.float_format = lambda x : '{:,.0f}'.format(x) if int(x) == x else '{:,.2f}'.format(x)


# In[21]:



conn = pyodbc.connect('DRIVER={SQL Server};SERVER=10.0.4.42,1433;DATABASE=MBLBI;UID=BIservice;PWD=BI@123')
sql_query = [
    #budget  variance
        '''select TBRANCH AS AC_BRANCH,[GROUP],CURRTOTAL AS YESTERDAY_TOTAL,CURRNOOF AS YESTERDAY_NOOF,(CURRTOTAL-PRVTOTAL)
		 AS DAILY_GROWTH,CURRNOOF-PRVNOOF AS DAILY_GROWTH_NOOF,CURRTOTAL-ASHAD_END_ACTUAL AS GROWTH_FROM_ASHAD,CURRNOOF-ASHAD_END_NOOF 
		 AS GROWTH_NOOF_FRM_ASHAD,CURRTOTAL-(ANNUAL_BUDGET+ASHAD_END_ACTUAL) AS BUDGET_VARIANCE,
		 FORMAT((CURRTOTAL-(ANNUAL_BUDGET+ASHAD_END_ACTUAL))/(NULLIF(ANNUAL_BUDGET+ASHAD_END_ACTUAL,0))*100,'N2')+' %'  as Variance_Pct_Change
		 ,ANNUAL_BUDGET+ASHAD_END_ACTUAL 
		 AS BUDGETED_TILL_MONTH,ASHAD_END_ACTUAL,ASHAD_END_NOOF from DEPOSIT_VARIANCE1_BRANCH  ''',
        
    #Loan variance
         '''    select TBRANCH AS AC_BRANCH,SEGMENT AS [GROUP],CURRLIMIT AS YESTERDAY_LIMIT,CURROUT
		 AS YESTERDAY_OUTS,CURRNOOFF AS YESTERDAY_NOOF,(CURROUT-PRVOUT) AS DAILY_GROWTH,(CURROUT-ASHAD_END_ACTUAL) 
		 AS GROWTH_FRM_ASHAD,CURROUT-(MONTHLY_BUDGET+ASHAD_END_ACTUAL) AS BUDGET_VARIANCE,
		 format((CURROUT-(MONTHLY_BUDGET+ASHAD_END_ACTUAL))/ nullif((MONTHLY_BUDGET+ASHAD_END_ACTUAL),0)*100,'N2')+' %' as Variance_Pct_Change,
		 MONTHLY_BUDGET+ASHAD_END_ACTUAL 
		 AS BUDGETED_OS_TILL_MONTH,ASHAD_END_ACTUAL,ASHAD_END_NOOF from DAILY_BRANCH_LOAN_BUDGET_SEGMENT''',
    #Ratio
        "select BRANCH_NAME AS AC_BRANCH,Particulars,ACTUAL*100 AS Actual,variance*100 as variance,budget*100 as budget,ashad_end_actual*100 as ashad  FROM BUDGET_variance_MONTHLY_FNL WHERE total_desc='Ratio'",

  #Account Mela Account
        "select AC_BRANCH,ACCOUNT_TYPE,NOOF AS NOOF_ACC_OPENED ,MIN_TARGET AS ACC_TARGET, Total_Points,remaining_days from ACCOUNT_MELA_FINAL",
 
    #NII
         "select AC_BRANCH,TOTAL_DESC AS Particulars,Actual,variance,budget,ashad from DAILY_BRANCH_NII_TMP  order by sn",
    #OD utilization
                '''select TBRANCH as AC_BRANCH,SUM(TOTAL_LIMIT_NPR) AS TOTAL_LIMIT,
        SUM(TOTAL_OUTSTANDING) AS OUTSTANDING,SUM(TOTAL_LIMIT_NPR)-SUM(TOTAL_OUTSTANDING) AS UNUTILISE,
        FORMAT(SUM(TOTAL_OUTSTANDING)/SUM(TOTAL_LIMIT_NPR)*100,'N2')+' %' AS UTILISE_PERCENT,
        format((SUM(TOTAL_LIMIT_NPR)-SUM(TOTAL_OUTSTANDING))/SUM(TOTAL_LIMIT_NPR)*100,'N2')+' %' AS UNUTILISE_PERCENT from MIS_LOAN_TMP_FINAL1 
         where  (BANK_DATE >= dateadd(day,-2,getdate())) and TBRANCH<>'BNK' AND LIMIT_REF_GROUP='100'
         GROUP BY TBRANCH''',

  #DAILY LOAN SETTTLED
        '''SELECT AC_BRANCH,AC_BRANCH AS [GROUP], CONTRACT,CUST_NAME,SEGMENT,CPGTYPE,OPEN_DATE,BANK_DATE, TOTAL_AMNT AS TOTAL,REMARKS 
         FROM LOAN_WISE_TREND_TMP WHERE BANK_DATE>=DATEADD(DAY,-2,GETDATE()) AND remarks='closed' and segment not like '%HO%' ''',

 #DAILY LOAN DISBURSEMENT
        '''SELECT AC_BRANCH,AC_BRANCH AS [GROUP], CONTRACT,CUST_NAME,SEGMENT,CPGTYPE,OPEN_DATE,BANK_DATE, TOTAL_AMNT AS TOTAL,REMARKS 
         FROM LOAN_WISE_TREND_TMP WHERE BANK_DATE>=DATEADD(DAY,-2,GETDATE()) AND remarks='NEW' and segment not like '%HO%' ''',

       
    # Income
          "select BRANCH_NAME AS AC_BRANCH,Particulars,ACTUAL AS Actual,variance as variance, budget,ashad_end_actual as ashad FROM BUDGET_variance_MONTHLY_FNL WHERE (total_desc='Transaction Income' or total_desc ='Fee, Commission & Other Income ' or total_desc='FOREX Income')",
    #Expenses
         "select BRANCH_NAME AS AC_BRANCH,Particulars,ACTUAL AS Actual,variance as variance, budget,ashad_end_actual as ashad  FROM BUDGET_variance_MONTHLY_FNL WHERE (total_desc='Interest Expenses_Deposits' or total_desc ='Expenses ')",

     #Activity SUMMARY
        "select branch as AC_BRANCH,activity_type as [GROUP],WEEKLY_GROWTH,MONTHLY_GROWTH,GROWTH_FRM_ASHAD,CAST(BUDGET_VARIANCE AS int) as BUDGET_VARIANCE,TOTAL_ACTUAL,ASHAD_END_ACTUAL,cast(month_cumu_budget as int) as MONTHLY_CUMULATIVE_BUDGET  from daily_activity_budget_variance_tmp",

    
        
    #Cash Position
        "SELECT COMP AS AC_BRANCH,LINE_NAME AS [GROUP], BANK_DATE, CCY AS CURRENCY, LCY_OPEN_BAL ,LCY_CLOSE_BAL FROM MBL_LIQUIDITY WHERE (MIS_GROUP_NAME LIKE '%CASH%') AND BANK_DATE>=DATEADD(DAY,-2,GETDATE())",
    #DR MOVEMENT
             "select AC_BRANCH,ACCOUNT,[NAME],CATEG_NAME,CURR_TOTAL AS BALANCE,DIFF AS DR_CR_MOVEMENT,BANK_DATE from DEPOSIT_MVMT_FNL where diff<=-1000000  ORDER BY DIFF",
    #CR MOVEMENT
             "select AC_BRANCH,ACCOUNT,[NAME],CATEG_NAME,CURR_TOTAL AS BALANCE,DIFF AS DR_CR_MOVEMENT,BANK_DATE from DEPOSIT_MVMT_FNL where diff>=1000000 ORDER BY DIFF",
    #CATCH ALL
             "select COMP AS AC_BRANCH,LINE_NAME,LCY_OPEN_BAL,LCY_CLOSE_BAL,BANK_DATE from MBLGL_TMP where BANK_DATE>=DATEADD(DAY,-2,GETDATE()) and GL_LINE='MACGL.9999' AND LCY_CLOSE_BAL IS NOT NULL AND CCY='LOCAL'",
    #TELLER TXN
             "SELECT TBRANCH AS AC_BRANCH,TELLID AS [GROUP], TELLID AS TELLER_ID,COUNT(*) AS NOOF,sum(LCYAMT1) AS TOTAL_TXN_AMOUNT,BNK_DATE AS BANK_DATE FROM TELLER_TMP WHERE BNK_DATE>=DATEADD(DAY,-2,GETDATE())GROUP BY TBRANCH,TELLID,BNK_DATE",
    #FT TXN
             "select A.TBRANCH as AC_BRANCH,A.FT_TXN_TYPE,B.TXN_DESC AS Transaction_Type,COUNT(*) AS NOOF,sum(DEBITED_AMT) as TXN_AMOUNT,BANK_DATE from FT_TMP A INNER JOIN ft_txn_type B ON A.FT_TXN_TYPE=B.TXN_ID where A.BANK_DATE>=DATEADD(DAY,-2,GETDATE()) GROUP BY a.TBRANCH,A.FT_TXN_TYPe, b.TXN_DESC,A.BANK_DATE",
    #CLEARING TXN
            '''SELECT A.CLR_BANK_CODE AS CLEARING_BANK,B.AC_BRANCH AS AC_BRANCH,A.FT_DR_ACC_NO AS DEBIT_ACCOUNT,
             A.DR_CUST_NAME,A.DAMOUNT_AMT AS DEBIT_AMOUNT,A.DVALUE_DATE,A.FTID AS TXN_ID,C.MOBILE,A.BANK_DATE
              FROM  FT_TMP A INNER JOIN ACCOUNT_LIVE B ON A.FT_DR_ACC_NO=B.ACCOUNT
              LEFT JOIN CUSTOMER_LIVE C on c.CUSTOMER = b.CUSTOMER
               WHERE (A.BANK_DATE >= dateadd(day,-2,getdate()) 
              AND (A.FT_TXN_TYPE ='ACCL')) AND A.DAMOUNT_AMT>='1000000' ''',
    #M3 CUSTOMER 
             "select left(BRANCH_CODE,3) as AC_BRANCH,STATUS,ACTIVE,COUNT(*) AS NOOF from M3_CUSTOMERS_NEW where ACTIVE='Y' AND CAST(CREATED_DATE AS DATE)>=DATEADD(DAY,-2,GETDATE()) GROUP BY BRANCH_CODE,STATUS,ACTIVE",
    #REMITTANCE TXN
             '''SELECT A.TBRANCH AS AC_BRANCH,B.[REMITTANCE ACCOUNT NAME] AS [GROUP],SUM(A.DR_TXN) AS DR_AMOUNT,SUM(A.CR_TXN) AS CR_AMOUNT,COUNT(*) AS NOOF
                 FROM Remitance_reconsilation1 A 
            INNER JOIN REMIT_ACC B ON
            A.[account number]=B.[ACCOUNT NUMBER]
            WHERE CAST(a.BANK_DATE AS DATE)>=DATEADD(DAY,-2,GETDATE()) 
            GROUP BY A.TBRANCH,B.[REMITTANCE ACCOUNT NAME]''',
    #ACCOUNT OPEN
            "select AC_BRANCH,CATEG_NAME AS [GROUP],SUM(AMOUNT) AS AMOUNT,COUNT(*) AS NOOF,BANK_DATE from account_live where OPEN_DATE>=dateadd(day,-2,getdate()) GROUP BY AC_BRANCH,CATEG_NAME,BANK_DATE",
    #FIXED deposit open
            "SELECT A.TBRANCH as AC_BRANCH, A.MMID, A.CUSTNO, B.NAME, A.CATEGORY, A.AMOUNTPR, A.OPEN_DATE, A.MAT_DATE, A.RATE, A.INT_AMOUNT, A.TAX_AMOUNT, A.CUST_REF, A.CURRENCY,BNK_DATE AS BANK_DATE FROM MM_TMP AS A INNER JOIN CUSTOMER_LIVE AS B ON A.CUSTNO = B.CUSTOMER WHERE (A.BNK_DATE >= DATEADD(day, - 2, GETDATE())) AND (A.OPEN_DATE >= DATEADD(day, -2, GETDATE())) AND (A.STATUS <> 'LIQ')", 
    #FIXED DEP MATURING
             "SELECT A.TBRANCH as AC_BRANCH, A.MMID, A.CUSTNO, B.NAME, A.CATEGORY, A.AMOUNTPR, A.OPEN_DATE, A.MAT_DATE, A.RATE, A.INT_AMOUNT, A.TAX_AMOUNT, A.CUST_REF, A.CURRENCY,MOBILE,BNK_DATE AS BANK_DATE FROM MM_TMP AS A INNER JOIN CUSTOMER_LIVE AS B ON A.CUSTNO = B.CUSTOMER WHERE (A.BNK_DATE >= DATEADD(day, - 2, GETDATE())) AND (A.MAT_DATE <= DATEADD(day, 10, GETDATE())) AND (A.STATUS <> 'LIQ') ORDER BY A.MAT_DATE",
    #NOOF CUSTOMER and Account status
            "SELECT AC_BRANCH,SAVING_GROUP AS [GROUP],ACTIVE_ACCOUNT,INACTIVE_ACCOUNT,KYC_NOT_UPDATED,KYC_UPDATED FROM BRANCH_DAILY_CUST_ACCT",
    #ACCOUNT CLOSED
         "SELECT BRANCH AS AC_BRANCH,ACCOUNT AS CLOSED_ACCOUNT, TOTAL_ACC_AMT as TOTAL_AMOUNT,SETTLEMENT_ACCOUNT,CLOSED_REASON,BANK_DATE FROM ACCT_CLOSED_FNL WHERE BANK_DATE>=DATEADD(DAY,-2,GETDATE())",
    #ATM Transactions
        "select A.TBRANCH as AC_BRANCH,A.TBRANCH AS [GROUP],SUM(A.NOOF) AS NOOF_TXN,SUM(A.DAMOUNT_AMT) AS TOTAL_AMOUNT,B.[ATM NAME] from ATM_FT_TMP A INNER JOIN ATM_DETAILS B ON A.FT_TERMID=B.[ATM  TERMINAL ID] AND A.TBRANCH=B.[Branch Short Code] where A.DVALUE_DATE>=dateadd(day,-2,getdate())GROUP BY A.TBRANCH,B.[ATM NAME]",
    #LOAN EXPIRING   
        "SELECT TBRANCH AS AC_BRANCH,CONTRACT,CUSTOMER,[NAME],OPEN_DATE,CATEGORY,LIMIT,OUTSTANDING_AMT,LIMIT_EXP AS LIMIT_EXPIRTY FROM MIS_LOAN_BRANCH_DAILY WHERE BANK_DATE>=DATEADD(DAY,-2,GETDATE()) AND LIMIT_EXP>=DATEADD(DAY,1,GETDATE()) AND LIMIT_EXP<=DATEADD(DAY,7,GETDATE()) AND ACC_OFFICER_BRANCH<>'KTM' ORDER BY LIMIT_EXPIRTY",

    #LC Expired Details
        "SELECT  BRANCH AS AC_BRANCH, ID, LC_NO,CUST_NO, CUST_NAME, BENEF, ISSUE_DATE,SYSTEM_EXP AS LC_EXPIRY_DATE,MAT_DATE as [SYSTEM_EXP_DATE], LC_CLOSE_DATE AS HISTORY_EXP_DATE,CCY, LC_AMOUNT,LC_LIAB_AMOUNT FROM LC_TMP WHERE BANK_DATE> = dateadd(day,-2,getdate()) AND SYSTEM_EXP <= dateadd(day,-2,getdate()) AND BRANCH<>'KTM' ",

    #Acceptance maturity (weekly)
        "SELECT DISTINCT BRANCH as AC_BRANCH, ID, CCY, CUST_NO,DRAW_ACC, CUST_NAME, TYPE, MAT_DATE AS [EXPIRY_DATE], REFNO AS LC_NO, PROV_AMT1 AS PROVISION_AMOUNT, DRAW_ACC, LC_AMT AS DRAWING_AMOUNT FROM  DRAWING_TMP WHERE MAT_DATE> =dateadd(day,-1,getdate())and MAT_DATE<=dateadd(day,7,getdate()) AND (TYPE='AC') AND BANK_DATE>dateadd(day,-2,getdate()) AND BRANCH<>'KTM' ",

    #Gaurantee EXPIRTED
        "SELECT BRANCH as AC_BRANCH, ID, CUST_NO, CUST_NAME, AMOUNT, MD_LIMIT, REF_NO, DEPARTMENT, MAT_DATE AS [EXPIRY_DATE], CCY, PROV_AMT,[TYPE] FROM MD_TMP WHERE MD_STATUS IS NULL AND cast(MAT_DATE as date) <= dateadd(day,-1,getdate()) AND BANK_DATE>= dateadd(day,-2,getdate()) AND BRANCH<>'KTM' ",
    #LOAN CLOSED
        "select TBRANCH AS AC_BRANCH,SEGMENT_DESC,CONTRACT,NAME,RATE,CATEGORY,OPEN_DATE,TOTAL_OUTSTANDING,CLOSED_DATE from DAILY_BRANCH_LOAN_CLOSED  WHERE CLOSED_DATE>=DATEADD(DAY,-2,GETDATE()) AND TBRANCH<>'KTM'order by closed_date,SEGMENT_DESC ",
    #DEPOSIT PRODUCT MOVEMENT
        "select B.MNEMONIC AS AC_BRANCH,B.MNEMONIC AS [GROUP],SUM(A.DRCR) AS DRCR_MOVEMENT,A.LINE_NAME AS PRODUCT_NAME,SUM(A.OPENBAL) AS OPENING_BALANCE,SUM(A.CLOSEBAL) AS CLOSING_BALANCE,A.BANK_DATE from GL_DEPOSIT_DRCR_MOVEMENT_WKLY A INNER JOIN MBL_BRANCH B ON A.BRANCH=B.[NAME] WHERE A.BANK_DATE>=DATEADD(DAY,-2,GETDATE()) AND A.DRCR<>0 GROUP BY A.BRANCH,B.MNEMONIC,A.LINE_NAME,A.BANK_DATE ", 
    #GL Dirrerence
        "select COMP AS AC_BRANCH,SUM(LCY_CLOSE_BAL) AS GL_DIFFERENCE from MBLGL_TMP WHERE BANK_DATE>=DATEADD(DAY,-2,GETDATE()) AND RIGHT(GL_LINE,4)<='5860' AND GL_LINE LIKE 'MACGL%' AND CCY='LOCAL' GROUP BY COMP ",
    #ATM CASH
        "select AC_BRANCH,ACCOUNT,[NAME] AS ACCOUNT_NAME,AMOUNT AS OUTSTANDING_BALANCE from account_live where category='10103' and amount is not null and [name] not like 'RECORD.AUTOMATICA%'",
       #OVERDRAWN ACCOUNT
        "SELECT AC_BRANCH,ACCOUNT,CUSTOMER,[NAME],[DESC] AS ACC_TYPE,CURRBAL AS CURRENT_BAL,PRVBAL AS PREVIOUS_BAL,REMARKS FROM ACC_OVRD_ACC_TMP_1 ORDER BY [DESC]",
    #PL Transaction Daily by Using (TT,FT and DC) module
        "SELECT AC_BRANCH,BANK_DATE,NARRATIVE AS TXN_TYPE,AMOUNT,NOOF as NOOF_TXN,TXN_MODE FROM CATEG_ENTRY_DAILY ORDER BY TXN_MODE",
    #Net  income perday
      #  "SELECT COMP AS AC_BRANCH,SUM(NII) AS Net_Interest_Income_Perday FROM DAILY_NII_FNL WHERE BANK_DATE>=dateadd(day,-2,getdate()) group by comp",
             ]

# In[5]:

remarks = ["Deposit Actual vs Budget",
           "Loan Actual vs Budget",
           "Ratio ",
           "Account Mela Account",
           "NII",
           "OD utilization %",
           "Daily Loan settlement",
           "Daily Loan Disbursement",
           "Income",
           "Expenses",
           "Activity Actual vs Budget",
           "Cash Position",
           "Deposit Debit Movement 1M above Daily",
           "Deposit Credit Movement 1M above Daily",
           "GL and Account not Mapping(Catch all)",
           "TELLER Transaction Daily",
           "Fund Transfer Transaction Daily ",
           "Inward Clearing Transaction 1M Above Daily",
           "M3 Enrolled Daily",
           "Remittance Transaction Daily",
           "Account Opened Daily",
           "Fixed Deposit Opened Daily",
           "Fixed Deposit Maturing Within 10 days",
           "Account and Customer KYC Status",
           "Account Closed Daily",
           "ATM Transactions Daily",
           "Loan Expiring Weekly",
           "LC Expired Details",
           "Acceptance Maturity (weekly)",
           "Gaurantee Expired",           
           "Loan Closed Daily",
           "Deposit Product Movement Daily",
           "GL Difference ",
           "ATM Cash Status Daily",
           "Overdrawn Accounts",
           "PL Transactions Daily through TT,FT and DC module",
           #"NII  Net Interest Income Daily ",
           ]


# In[22]:


path = "C:\html table\\branch\\"
import os
import sys
import shutil

# Get directory name
## Try to remove tree; if failed show an error using try...except on screen
try:
    shutil.rmtree(path)
    print ("Succesfull delete directory:"+path)
except OSError as e:
    print ("Error: %s - %s." % (e.filename, e.strerror))

try:
    os.makedirs(os.path.dirname(path))
    print ("Succesfull make directory:"+path)
except OSError as e:
    print("cannot made derectory:"+path)


# In[23]:


##branch_df = pd.read_sql("SELECT * FROM EMAIL_ADD",conn)
branch_df1 = pd.read_sql("SELECT * FROM BM_DETAILS WHERE BRANCH_CODE IS NOT NULL AND EMAIL IS NOT NULL",conn)
branch_df1.sort_values("BRANCH_CODE",inplace = True)
branch_name = list(branch_df1.BRANCH_CODE)
print(len(branch_name))
unique_branch_name =branch_df1.BRANCH_CODE.unique()
print(unique_branch_name)

##bm_name =branch_df.NAME.str.split(" ", expand = True)

bm_name =branch_df1.Branch_Managers.str.split(" ", expand = True)

branch_df1['firstname'] = branch_df1.Branch_Managers.apply(lambda x:x.split(" ")[0])
bm_name = list(branch_df1.firstname)
print(bm_name)


# In[24]:


HEADER = '''
<html>
    <head>
        <style>
           table {
              font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
              border-collapse: collapse;
              table-layout: auto;
              font-size :12px;
                }


         td,  th {
            border: 1px solid #ddd;
            padding: 12px;
            font-size :12px;
            text-align: right;
            
                }


        tr:hover {background-color: #ddd;}
        tbody tr:last-child {
    background-color: wheat;
}

        thead {
          padding-top: 12px;
          padding-bottom: 12px;
        text-align: center;
          background-color: #4CAF50;
          color: white;
          font-size :12px;
            }
        </style>
    </head>
    <body>
'''
FOOTER = '''
    </body>
    
</html>
'''


# In[25]:


#make separate html of each branch
for i in range(len(sql_query)):
    df = pd.read_sql(sql_query[i], conn)
    df.fillna(0,inplace = True)
    df1= df.copy()
    branch = df.AC_BRANCH.unique()
    my_dict = {}
    for name in branch:
        my_dict[name] = df1[df1['AC_BRANCH'] == name]
        try:
            my_dict[name].set_index("GROUP",inplace = True)
            my_dict[name] = my_dict[name].append(my_dict[name].sum(numeric_only=True).rename('Total'))
            my_dict[name].reset_index(inplace = True)
            my_dict[name].fillna("",inplace =True)
        except:
            pass
        my_dict[name].reset_index(inplace = True)
        my_dict[name] =my_dict[name].drop('index',axis =1)
        my_dict[name].index = np.arange(1,len(my_dict[name])+1)
        filename = path+str(my_dict[name]['AC_BRANCH'].iloc[0])+"\\"
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
            try:
                with open(filename+"\\"+my_dict[name]['AC_BRANCH'].iloc[0]+str(i)+'.html', 'w') as f:
                    try:
                        f.write('<h3 style=color:blue; font-weight:bold;>')
                        f.write(remarks[i])
                        f.write('</h3>')
                    except:
                        pass
                    f.write(my_dict[name].to_html(classes=''))
                    f.close()
            except:
                pass
        else:
            try:
                with open(filename+"\\"+my_dict[name]['AC_BRANCH'].iloc[0]+str(i)+'.html', 'w') as f:
                    try:   
                        f.write('<h3 style=color:blue; font-weight:bold;>')
                        f.write(remarks[i])
                        f.write('</h3>')
                    except:
                        pass
                    f.write(my_dict[name].to_html(classes=''))
                    f.close()
            except:
                pass


# # for visualization 

# In[26]:


sql_query1 = ["SELECT COMP as AC_BRANCH, SUM(LCY_CLOSE_BAL) AS BALANCE, BANK_dATE FROM GL_DEPOSIT_WEEKLY GROUP BY BANK_DATE,COMP",
            '''SELECT TBRANCH as AC_BRANCH,SUM(TOTAL_OUTSTANDING) AS BALANCE, BANK_dATE FROM MIS_LOAN_TMP_FINAL1 
where BANK_DATE >=dateadd(day,-15,GETDATE()) and segment not like '%HO%'
group by BANK_DATE,TBRANCH '''
             ]
remarks1 = ["15 Days Deposit Trend", "15 Days Loan Trend"]


# In[27]:


# import necessary packages 
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['axes.labelsize'] = 14
matplotlib.rcParams['xtick.labelsize'] = 12
matplotlib.rcParams['ytick.labelsize'] = 12
matplotlib.rcParams['text.color'] = 'k'
matplotlib.rcParams['figure.figsize'] = 20, 20


# In[28]:


#make visualization of each branch
import seaborn as sns
from matplotlib.ticker import FixedLocator
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
#make visualization of each branch
for i in range(len(sql_query1)):
    df = pd.read_sql(sql_query1[i], conn)
    df.fillna(0,inplace = True)
    df1= df.copy()
    df1['BANK_dATE'] = pd.to_datetime(df1['BANK_dATE'])
    df1.sort_values("BANK_dATE",inplace = True)
    df1.BALANCE = df1.BALANCE /1000000
    branch = df.AC_BRANCH.unique()
    my_dict1 = {}
    for name in branch:
        my_dict1[name] = df1[df1['AC_BRANCH'] == name]
        filename = path+my_dict1[name]['AC_BRANCH'].iloc[0]+"\\"

        fig, ax = plt.subplots(figsize=(20,5))
        X=my_dict1[name].BANK_dATE
        Y=my_dict1[name].BALANCE
        # add the x-axis and the y-axis to the plot
        locator = mdates.DayLocator(bymonthday=[i for i in range(1,32)])
        formatter = mdates.DateFormatter('%b %d')
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
        ax.plot(X, Y, color = 'green')
        # rotate tick labels
        plt.title("TREND OF "+remarks1[i])
        plt.xlabel("Date")
        plt.ylabel('Amount in Million')
        date = list(my_dict1[name].BANK_dATE)
        value = list(my_dict1[name].BALANCE)
        for x,y in zip(X,Y):
            label = "{:,.1f}".format(y)+"M"

            plt.annotate(label, # this is the text
                         (x,y), # this is the point to label
                         textcoords="offset points", # how to position the text
                         xytext=(0,10), # distance from text to points (x,y)
                         ha='center',
                        size = 16)
        
        fig.savefig(path+"\\"+my_dict1[name]['AC_BRANCH'].iloc[0]+"\\test"+str(i)+".png")
        plt.close(fig)
#         html = '<img src="cid:0">'.format()
        with open(filename+"\\"+my_dict1[name]['AC_BRANCH'].iloc[0]+'999.html', 'a+') as f:  
            f.write('<h3>')
            f.write(remarks1[i])
            f.write('</h3><br><br>')
            f.write('<img src ="cid:'+str(i)+'"'+'/>')
            f.close()




# In[29]:


# join each file and make final html file
import os
list_of_files = {}
list_of_names =[]
for (dirpath, dirnames, filenames) in os.walk(path):
    for filename in filenames:
        only_name = filename.split('.')[0]
        name = " ".join(re.findall("[a-zA-Z_]+", only_name))
        list_of_names.append(name)
        if filename.endswith('.html'): 
            list_of_files[filename] = os.sep.join([dirpath, filename])

file_listed = []
import glob
for i in range(len(list_of_names)):
    file_list = glob.glob(path+list_of_names[i]+"\\*.html")
    file_listed.append(file_list)
    
for i in range(len(list_of_names)):
    try:
        with open(path+list_of_names[i]+"\\"+list_of_names[i]+".html", 'w') as outfile:
            outfile.write(HEADER)
            for z in range(len(branch_name)):
                if list_of_names[i] == branch_name[z]:
                    outfile.write('<p> Dear <strong>')
                    outfile.write(bm_name[z])
                    outfile.write(' jee</strong>,<br/><br/> &emsp;&emsp; Please Review the Daily Branch Overview. If any further ammendment is required kindly suggest us.</p><br/> Thank you,<br/> <strong>Business Intelligence Department</strong><br/>')
            for j in file_listed[i]:
                outfile.write('\n<br>\n')
                with open(j) as infile:
                    for line in infile:
                        outfile.write(line)
            outfile.write(FOOTER)
            outfile.close()
    except:
        pass


# # For Mail

# In[30]:


import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pyodbc
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

file_to_send = []
import glob
for bn in branch_name:
    f = glob.glob(path+str(bn)+"\\"+str(bn)+".html")
    print(f)
    try:
        file_to_send.append(f[0])
    except:
        file_to_send.append("")


# In[145]:
print("hello")
print(len(branch_df1),len(file_to_send))
to_send_list = list(branch_df1.Email)
print(len(to_send_list),len(file_to_send))
branch_df1['file'] = file_to_send
print(branch_df1['file'])


# In[19]:


for i in range(len(file_to_send)):
    try:
        me = "sender_email"
        you = [to_send_list[i]]
        try:
            you=you[0].split(",")
        except:
            pass
        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Branch Daily Overview"
        msg['From'] = me
        msg['To'] =  ', '.join(you)
        
        filename = file_to_send[i]
        f = open(filename)
        attachment = MIMEText(f.read(),'html')
        msg.attach(attachment)
        
        
            # to add an attachment is just add a MIMEBase object to read a picture locally.
        try:
            for j in range(len(sql_query1)):
                with open(path+branch_name[i]+'\\test'+str(j)+'.png', 'rb') as f:
                    print(path+branch_name[i]+'\\test'+str(j)+'.png')
                    # set attachment mime and file name, the image type is png
                    mime = MIMEBase('image', 'png', filename='test'+str(i)+'.png')
                    # add required header data:
                    mime.add_header('Content-Disposition', 'attachment', filename='test'+str(j)+'.png')
                    mime.add_header('X-Attachment-Id', str(j))
                    mime.add_header('Content-ID', '<'+str(j)+'>')
                    # read attachment file content into the MIMEBase object
                    mime.set_payload(f.read())
                    # encode with base64
                    encoders.encode_base64(mime)
                    # add MIMEBase object to MIMEMultipart object
                    msg.attach(mime)
        except:
            pass
     
        # Send the message via local SMTP server.
        s = smtplib.SMTP('server_host_id'{eg:mail.com})
        s.sendmail(me, you, msg.as_string())
        s.quit()
        f.close()
        print("emIl_send_succesfully")
    except:
        pass


# In[ ]:




