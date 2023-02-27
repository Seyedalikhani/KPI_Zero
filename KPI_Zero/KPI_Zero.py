import ftplib
import patoolib
import os
import glob
from zipfile import ZipFile
import pandas as pd
import pyodbc
import datetime




# ******************************** Thresholds *********************************
CSSR_2G=92
CDR_2G=3
IHSR_2G=92
OHSR_2G=92
Traffic_2G=0
Availability_2G=99

CS_Traffic_3G=0
CS_RAB_3G=95
CS_RRC_3G=95
CS_Drop_3G=4
Availability_3G=99
PS_Traffic_3G=0
PS_RRC_3G=95
PS_Drop_3G=4
RSSI_3G=-90
THR_3G=1

ERAB_Setup_SR_4G = 96
UE_DL_THR_4G = 8
Cell_Availability_4G = 99
CSFB_Success_Rate_4G = 95
Volte_Traffic_4G = 0
ERAB_Drop_Rate_4G = 3
Intra_Freq_HO_SR_4G = 95
RRC_Connection_SR_4G = 96
PS_Traffic_4G = 0


Run_2G=True
Run_3G=False
Run_4G=False


Text_Log = open("D:\P1\Performane\Programmes\Python Projects\KPIZero\KPI_Zero\KPI_Zero\Logfile.txt", "r")


# (((((((((((((((((((((((((((( Functions ))))))))))))))))))))))))))))))))))))))))))
def string_date(date):
    if (date.month<=9):
        month_str="0"+str(date.month)
    else:
        month_str=str(date.month)
    if (date.day<=9):
        day_str="0"+str(date.day)
    else:
        day_str=str(date.day)
    Day=str(date.year)+"-"+month_str+"-"+day_str
    return(Day)

def string_date_2(date):
    if (date.month<=9):
        month_str="0"+str(date.month)
    else:
        month_str=str(date.month)
    if (date.day<=9):
        day_str="0"+str(date.day)
    else:
        day_str=str(date.day)
    Day=str(date.year)+month_str+day_str
    return(Day)




# ******************************** KPI0 Interval *********************************
Day_back_ind=-4
Sub_date=datetime.datetime.now() + datetime.timedelta(Day_back_ind)
Sub_date_days = string_date(Sub_date)
Currect_date=datetime.datetime.now()
Currect_date_day=string_date(Currect_date)



# ******************************** Database Connection *********************************
# Connection to apache_nifi
conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                      'Server=apache_nifi;'
                      'Database=Desk;'
                      'Trusted_Connection=yes;')
conn_apache_Desk = conn_apache_Desk.cursor()


# Connection to PERFORMANCEDB01
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=PERFORMANCEDB01;'
                      'Database=Performance_NAK;'
                      'Trusted_Connection=yes;')
conn_performanceDB = conn.cursor()


# Connection to apache_nifi
conn_apache_Spring = pyodbc.connect('Driver={SQL Server};'
                      'Server=apache_nifi;'
                      'Database=Spring;'
                      'Trusted_Connection=yes;')
conn_apache_Spring = conn_apache_Spring.cursor()

# ******************************** Date List ********************************
Num_KPI0_CheckDay=0
Day_back_ind=0
KPI0_CheckDay=[]
while Num_KPI0_CheckDay<=2:
      Day_back_ind-=1
      sub_date_days = datetime.datetime.now() + datetime.timedelta(Day_back_ind)
      Num_KPI0_CheckDay+=1
      KPI0_CheckDay.append(sub_date_days)

date_list1=""
date_list2=""
date_list1_Nokia_G1800=""
date_list2_Nokia_G1800=""
date_list1_lte=""
date_list2_lte=""
date_list1_lte_volte=""
date_list2_lte_volte=""

for d in range(3):
    Day=string_date(KPI0_CheckDay[d])
    Day_Nokia_G1800=string_date_2(KPI0_CheckDay[d])
    if d==0 or d==1:
        date_list1 = date_list1 + "substring(convert(varchar, Date, 23), 1, 10) = '" + Day + "' or ";
        date_list1_lte = date_list1_lte + "substring(convert(varchar, Datetime, 23), 1, 10) = '" + Day + "' or ";
        date_list1_Nokia_G1800 = date_list1_Nokia_G1800 + "Date_Key = '" + Day_Nokia_G1800 + "' or ";
    if d==1 or d==2:
        date_list2 = date_list2 + "substring(convert(varchar, Date, 23), 1, 10) = '" + Day + "' or ";
        date_list2_lte = date_list2_lte + "substring(convert(varchar, Datetime, 23), 1, 10) = '" + Day + "' or ";
        date_list2_Nokia_G1800 = date_list2_Nokia_G1800 + "Date_Key = '" + Day_Nokia_G1800 + "' or ";

date_list1=date_list1[0:len(date_list1)-4];
date_list2=date_list2[0:len(date_list2)-4];
date_list1_Nokia_G1800=date_list1_Nokia_G1800[0:len(date_list1_Nokia_G1800)-4];
date_list2_Nokia_G1800=date_list2_Nokia_G1800[0:len(date_list2_Nokia_G1800)-4];
date_list1_lte=date_list1_lte[0:len(date_list1_lte)-4];
date_list2_lte=date_list2_lte[0:len(date_list2_lte)-4];

# ******************************** 2G *********************************

# ----------------------------- 2G CM Table -------------------------------


if Run_2G:

    conn_apache_Desk.execute("select id, province, location, site, tech, bscRnc,  vendor, [integrationStatus], [rssvDate], CMStatus , [kpiZDate],  [kpiZStatus] from CM where [integrationStatus]='Fully Integrated' and ([kpiZStatus]!='pass') and (substring(tech,2,1)='G')" +
    " union all "+
    "select  id, province, location, site, tech, bscRnc,  vendor, [integrationStatus], [rssvDate], CMStatus , [kpiZDate],  [kpiZStatus] from CM where [integrationStatus]='Fully Integrated' and ([kpiZStatus] is null) and (substring(tech,2,1)='G')")
    CM_2G_Table=conn_apache_Desk.fetchall()

        #conn_apache_Desk.execute("select id, province, location, site, tech, bscRnc,  vendor, [integrationStatus], [rssvDate], CMStatus , [kpiZDate],  [kpiZStatus] from CM where [integrationStatus]='Fully Integrated' and ([kpiZStatus] is null or [kpiZStatus]!='Pass') and (substring(tech,2,1)='G')  and cast([rssvDate] as Date)>='"+ Sub_date_days +"' and cast([rssvDate] as Date)<='"+Currect_date_day+ "'")


    Ericsson_2G_list=""
    Huawei_TH_2G_list=""
    Huawei_2G_list=""
    Nokia_G1800_list=""
    Nokia_G900_list=""

    for i in range(len(CM_2G_Table)):


        Row_2G=str(CM_2G_Table[i])
        Row_2G=Row_2G.split(", ")

        # Finding column number of headers 
        location_header_ind=0
        site_header_ind=0 
        tech_header_ind=0
        vendor_header_ind=0
        for k in range(len(CM_2G_Table[i].cursor_description)):
            hearder_string=str(CM_2G_Table[i].cursor_description[k])
            if hearder_string[2:10]=="location":
                location_header_ind=k
            if hearder_string[2:6]=="site":
                site_header_ind=k
            if hearder_string[2:6]=="tech":
                tech_header_ind=k
            if hearder_string[2:8]=="vendor":
                vendor_header_ind=k

        if location_header_ind==0:
            Location=""
        else:
            Location=Row_2G[location_header_ind]
        if site_header_ind==0:
            Site=""
        else:
            Site=Row_2G[site_header_ind]
        if tech_header_ind==0:
            Band=""
        else:
            Band=Row_2G[tech_header_ind]
        if vendor_header_ind==0:
            Vendor=""
        else:
            Vendor=Row_2G[vendor_header_ind]


        if Vendor=="'Ericsson'":
            Ericsson_Site=Location[1:7]
            Ericsson_2G_list=Ericsson_2G_list+"substring([Cell],1,6)='" + Ericsson_Site + "' or ";
        if Vendor=="'Nokia'" and Band=="'2G-1800 MHz (GSM 1800)'":
            Nokia_G1800_Site=Site[1:9]
            Nokia_G1800_list=Nokia_G1800_list+"substring([Cell_Name],1,8)='" + Nokia_G1800_Site + "' or ";
        if Vendor=="'Nokia'" and Band=="'2G-900 MHz (GSM 900)'":
            Nokia_G900_Site=Site[1:9]
            Nokia_G900_list=Nokia_G900_list+"substring([Seg],1,8)='" + Nokia_G900_Site + "' or ";
        if Vendor=="'Huawei'" and Location[1:3]=="TH":
            Huawei_TH_Site=Location[1:7]
            Huawei_TH_2G_list=Huawei_TH_2G_list+"substring([Cell],1,6)='" + Huawei_TH_Site + "' or ";
        if Vendor=="'Huawei'" and Location[1:2]!="TH":
            Huawei_Site=Site[1:9]
            Huawei_2G_list=Huawei_2G_list+"substring([Cell],1,8)='" + Huawei_Site + "' or ";




    Ericsson_2G_list=Ericsson_2G_list[0:len(Ericsson_2G_list)-4];
    Nokia_G1800_list=Nokia_G1800_list[0:len(Nokia_G1800_list)-4];
    Nokia_G900_list=Nokia_G900_list[0:len(Nokia_G900_list)-4];
    Huawei_TH_2G_list=Huawei_TH_2G_list[0:len(Huawei_TH_2G_list)-4];
    Huawei_2G_list=Huawei_2G_list[0:len(Huawei_2G_list)-4];



    # ------------------------------ 2G KPI Tables -------------------------------

    if Ericsson_2G_list!="":
        conn_performanceDB.execute("select [Date], [BSC], 'Ericsson' as 'Vendor', substring(Cell,1,6) as 'Site', [Cell], [TCH_Traffic_BH] as 'TCH_Traffic_BH (Erlang)', [CSSR_MCI] as'CSSR', [CDR(not Affected by incoming Handovers from 3G)(Eric_CELL)] as 'Voice Drop Rate', [IHSR] as 'IHSR', [OHSR] as 'OHSR', [TCH_Availability] as 'TCH Availability' from [dbo].[CC2_Ericsson_Cell_BH] where  (" + Ericsson_2G_list + ") and (" + date_list1 + ")")
        KPI_Ericsson_2G_Table = conn_performanceDB.fetchall()
    if Huawei_TH_2G_list!="":
        conn_performanceDB.execute("select [Date], [BSC], 'Huawei' as 'Vendor', substring(Cell,1,6) as 'Site', [Cell], [TCH_Traffic_BH] as 'TCH_Traffic_BH (Erlang)', [CSSR3] as'CSSR', [CDR3] as 'Voice Drop Rate', [IHSR2] as 'IHSR', [OHSR2] as 'OHSR',  [TCH_Availability] as 'TCH Availability' from [dbo].[CC2_Huawei_Cell_BH] where  (" + Huawei_TH_2G_list + ") and (" + date_list1 + ")")
        KPI_Huawei_TH_2G_Table = conn_performanceDB.fetchall()
    if Huawei_2G_list!="":
        conn_performanceDB.execute("select [Date], [BSC], 'Huawei' as 'Vendor', substring(Cell,1,8) as 'Site', [Cell], [TCH_Traffic_BH] as 'TCH_Traffic_BH (Erlang)', [CSSR3] as'CSSR', [CDR3] as 'Voice Drop Rate', [IHSR2] as 'IHSR', [OHSR2] as 'OHSR',  [TCH_Availability] as 'TCH Availability' from [dbo].[CC2_Huawei_Cell_BH] where  (" + Huawei_2G_list + ") and (" + date_list1 + ")")
        KPI_Huawei_2G_Table = conn_performanceDB.fetchall()
    if Nokia_G900_list!="":
        conn_performanceDB.execute("select [Date], [BSC], 'Nokia' as 'Vendor', substring(Seg,1,8) as 'Site', [SEG] as 'Cell', [TCH_Traffic_BH] as 'TCH_Traffic_BH (Erlang)', [CSSR_MCI] as'CSSR', [CDR(including_CS_IRAT_handovers_3G_to2G)(Nokia_SEG)] as 'Voicde  Drop Rate', [IHSR] as 'IHSR', [OHSR] AS 'OHSR', [TCH_Availability] as 'TCH Availability' from [dbo].[CC2_Nokia_Cell_BH] where (" + Nokia_G900_list + ") and (" + date_list1 + ")")
        KPI_Nokia_G900_Table = conn_performanceDB.fetchall()
    if Nokia_G1800_list!="":
        conn_apache_Spring.execute("select [DATE_KEY], 'Null' as 'BSC', 'Nokia' as 'Vendor', substring(CELL_NAME,1,8) as 'Site', [CELL_NAME] as 'Cell', [TCH_Traffic] as 'TCH_Traffic_BH (Erlang)', [CSSR_MCI] as'CSSR', [Voice_Call_Drop_Rate] as 'Voicde  Drop Rate', [IHSR] as 'IHSR', [OHSR] AS 'OHSR', [TCH_Availability] as 'TCH Availability' from [Nokia_G1800] where (" + Nokia_G1800_list + ") and (" + date_list1_Nokia_G1800 + ")")
        KPI_Nokia_G1800_Table = conn_apache_Spring.fetchall()

    KPI0_2G_Results=[]
    n = len(CM_2G_Table)+1
    m = 6
    KPI0_2G_Results = [""] * n
    for x in range (n):
        KPI0_2G_Results[x] = [""] * m

    KPI0_2G_Results[0][0] ='ID'
    KPI0_2G_Results[0][1] ='Site'
    KPI0_2G_Results[0][2] ='Statue'
    KPI0_2G_Results[0][3] ='Update_Date'
    KPI0_2G_Results[0][4] ='Rejecteded_List'
    KPI0_2G_Results[0][5] ='NotUpdated_List'

    print(len(CM_2G_Table))

    for i in range(len(CM_2G_Table)):
        Row_2G=str(CM_2G_Table[i])
        Row_2G=Row_2G.split(", ")
        print(i)
        # Finding column number of headers 
        site_header_ind=0 
        for k in range(len(CM_2G_Table[i].cursor_description)):
            hearder_string=str(CM_2G_Table[i].cursor_description[k])
            if hearder_string[2:10]=="location":
                location_header_ind=k
            if hearder_string[2:6]=="site":
                site_header_ind=k
            if hearder_string[2:6]=="tech":
                tech_header_ind=k
            if hearder_string[2:8]=="vendor":
                vendor_header_ind=k
        ID=Row_2G[0]
        ID=int(ID[1:len(ID)])
        if location_header_ind==0:
            Location=""
        else:
            location=Row_2G[location_header_ind]
            location=location[1:len(location)-1]
        if site_header_ind==0:
            Site=""
        else:
            Site=Row_2G[site_header_ind]
            Site=Site[1:len(Site)-1]
        if tech_header_ind==0:
            Band=""
        else:
            Band=Row_2G[tech_header_ind]
        if vendor_header_ind==0:
            Vendor=""
        else:
            Vendor=Row_2G[vendor_header_ind]

        KPI_Table=[]
        if Vendor=="'Nokia'" and Band=="'2G-900 MHz (GSM 900)'":
            KPI_Table=KPI_Nokia_G900_Table 
        if Vendor=="'Nokia'" and Band=="'2G-1800 MHz (GSM 1800)'":
            KPI_Table=KPI_Nokia_G1800_Table 
        if Vendor=="'Huawei'" and Site[1:3]=="TH":
            KPI_Table=KPI_Huawei_TH_2G_Table
        if Vendor=="'Huawei'" and Site[1:3]!="TH":
            KPI_Table=KPI_Huawei_2G_Table 
        if Vendor=="'Ericsson'":
            KPI_Table=KPI_Ericsson_2G_Table


        Status="Pass"
        Rejecteded_List=""
        Not_Updated_List=""
        NotUpdated_List=""

        NotUpdated_TCH_Traffic_Count=0
        NotUpdated_CSSR_Count=0
        NotUpdated_CDR_Count=0
        NotUpdated_IHSR_Count=0
        NotUpdated_OHSR_Count=0
        NotUpdated_Availability_Count=0

        Cell_Count=0

        KPI0_2G_Results[i+1][0] =ID
        KPI0_2G_Results[i+1][1] =Site
        KPI0_2G_Results[i+1][2] =Status
        KPI0_2G_Results[i+1][3] =Currect_date_day
        KPI0_2G_Results[i+1][4] =Rejecteded_List
        KPI0_2G_Results[i+1][5] =NotUpdated_List



        if len(KPI_Table)==0:
            KPI0_2G_Results[i+1][0] =ID
            KPI0_2G_Results[i+1][1] =Site
            KPI0_2G_Results[i+1][2] ="No Data"
            KPI0_2G_Results[i+1][3] =Currect_date_day
            KPI0_2G_Results[i+1][4] =""
            KPI0_2G_Results[i+1][5] =""
        if len(KPI_Table)!=0:
            for p in range(len(KPI_Table)):
                Row_2G_KPI=str(KPI_Table[p])
                Row_2G_KPI=Row_2G_KPI.split(", ")
                if (len(Row_2G_KPI)==15):
                    G1800_Nokia_Flag=0
                    Candidate_Site=Row_2G_KPI[7]
                    Candidate_Site=Candidate_Site[1:len(Candidate_Site)-1]
                else:
                    G1800_Nokia_Flag=1
                    Candidate_Site=Row_2G_KPI[3]
                    Candidate_Site=Candidate_Site[1:len(Candidate_Site)-1]
                if (Vendor=="'Ericsson'"):
                    Site_CM=location
                else:
                    Site_CM=Site

                if (Candidate_Site==Site_CM and G1800_Nokia_Flag==1):
                    Date=Row_2G_KPI[0]
                    Date=Date[2:len(Date)-1]
                    Cell=Row_2G_KPI[4]
                    Cell=Cell[1:len(Cell)-1]
                    TCH_Traffic=Row_2G_KPI[5]
                    CSSR=Row_2G_KPI[6]
                    CDR=Row_2G_KPI[7]
                    IHSR=Row_2G_KPI[8]
                    OHSR=Row_2G_KPI[9]
                    Availability=Row_2G_KPI[10]

                    if (TCH_Traffic[0:4]!="None"):
                        TCH_Traffic=float("{:.2f}".format(float(TCH_Traffic[1:len(TCH_Traffic)-1])))
                    if (CSSR[0:4]!="None"):
                        CSSR=float("{:.2f}".format(float(CSSR[1:len(CSSR)-1])))
                    if (CDR[0:4]!="None"):
                        CDR=float("{:.2f}".format(float(CDR[1:len(CDR)-1])))
                    if (IHSR[0:4]!="None"):
                        IHSR=float("{:.2f}".format(float(IHSR[1:len(IHSR)-1])))
                    if (OHSR[0:4]!="None"):
                        OHSR=float("{:.2f}".format(float(OHSR[1:len(OHSR)-1])))
                    if (Availability[0:4]!="None"):
                        Availability=float("{:.2f}".format(float(Availability[1:len(Availability)-2])))

                    if type(TCH_Traffic)==float:
                        if (TCH_Traffic<=Traffic_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" TCH_Traffic at "+Date+": "+str(TCH_Traffic)+", "
                    else:
                        NotUpdated_TCH_Traffic_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" TCH_Traffic at "+Date+": "+str(TCH_Traffic)+", "
                    if type(CSSR)==float:
                        if (CSSR<CSSR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CSSR at "+Date+": "+str(CSSR)+", "
                    else:
                        NotUpdated_CSSR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" CSSR at "+Date+": "+str(CSSR)+", "
                    if type(CDR)==float:
                        if (CDR>CDR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CDR at "+Date+": "+str(CDR)+", "
                    else:
                        NotUpdated_CDR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" CDR at "+Date+": "+str(CDR)+", "
                    if type(IHSR)==float:
                        if (IHSR<IHSR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" IHSR at "+Date+": "+str(IHSR)+", "
                    else:
                        NotUpdated_IHSR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" IHSR at "+Date+": "+str(IHSR)+", "
                    if type(OHSR)==float:
                        if (OHSR<OHSR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" OHSR at "+Date+": "+str(OHSR)+", "
                    else:
                        NotUpdated_OHSR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" OHSR at "+Date+": "+str(OHSR)+", "
                    if type(Availability)==float:
                        if (Availability<Availability_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" Availability at "+Date+": "+str(Availability)+", "
                    else:
                        NotUpdated_Availability_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" Availability at "+Date+": "+str(Availability)+", "

                    Cell_Count+=1
                    if (Status=="Not Updated" and (NotUpdated_TCH_Traffic_Count==Cell_Count or NotUpdated_CSSR_Count==Cell_Count or NotUpdated_CDR_Count==Cell_Count or NotUpdated_IHSR_Count==Cell_Count or NotUpdated_OHSR_Count==Cell_Count or NotUpdated_Availability_Count==Cell_Count)):
                        Status='Not Updated'
                    else:
                       if Status!="Reject":
                           Status='Pass'

                    if Rejecteded_List!="":
                        Status="Reject"
                    KPI0_2G_Results[i+1][0] =ID
                    KPI0_2G_Results[i+1][1] =Site
                    KPI0_2G_Results[i+1][2] =Status
                    KPI0_2G_Results[i+1][3] =Currect_date_day
                    KPI0_2G_Results[i+1][4] =Rejecteded_List
                    KPI0_2G_Results[i+1][5] =NotUpdated_List


                if (Candidate_Site==Site_CM and G1800_Nokia_Flag==0):
                    Date_Year=Row_2G_KPI[0]
                    Date_Year=Date_Year[len(Date_Year)-4:len(Date_Year)]
                    Date_Month=Row_2G_KPI[1]
                    Date_Day=Row_2G_KPI[2]
                    if (int(Date_Month)<=9):
                        Date_Month="0"+Date_Month                
                    if (int(Date_Day)<=9):
                        Date_Day="0"+Date_Day
                    Date=Date_Year+"-"+Date_Month+"-"+Date_Day

                    Cell=Row_2G_KPI[8]
                    Cell=Cell[1:len(Cell)-1]
                    TCH_Traffic=Row_2G_KPI[9]
                    CSSR=Row_2G_KPI[10]
                    CDR=Row_2G_KPI[11]
                    IHSR=Row_2G_KPI[12]
                    OHSR=Row_2G_KPI[13]
                    Availability=Row_2G_KPI[14]
                    Availability=Availability[0:len(Availability)-1]

                    if (TCH_Traffic!="None"):
                        TCH_Traffic=float("{:.2f}".format(float(TCH_Traffic)))
                    if (CSSR!="None"):
                        CSSR=float("{:.2f}".format(float(CSSR)))
                    if (CDR!="None"):
                        CDR=float("{:.2f}".format(float(CDR)))
                    if (IHSR!="None"):
                        IHSR=float("{:.2f}".format(float(IHSR)))
                    if (OHSR!="None"):
                        OHSR=float("{:.2f}".format(float(OHSR)))
                    if (Availability!="None)"):
                        Availability=float("{:.2f}".format(float(Availability)))

                    if TCH_Traffic!="None":
                        if (TCH_Traffic<=Traffic_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" TCH_Traffic at "+Date+": "+str(TCH_Traffic)+", "
                    else:
                        NotUpdated_TCH_Traffic_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" TCH_Traffic at "+Date+": "+str(TCH_Traffic)+", "
                    if CSSR!="None":
                        if (CSSR<CSSR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CSSR at "+Date+": "+str(CSSR)+", "
                    else:
                        NotUpdated_CSSR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" CSSR at "+Date+": "+str(CSSR)+", "
                    if CDR!="None":
                        if (CDR>CDR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CDR at "+Date+": "+str(CDR)+", "
                    else:
                        NotUpdated_CDR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" CDR at "+Date+": "+str(CDR)+", "
                    if IHSR!="None":
                        if (IHSR<IHSR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" IHSR at "+Date+": "+str(IHSR)+", "
                    else:
                        NotUpdated_IHSR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" IHSR at "+Date+": "+str(IHSR)+", "
                    if OHSR!="None":
                        if (OHSR<OHSR_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" OHSR at "+Date+": "+str(OHSR)+", "
                    else:
                        NotUpdated_OHSR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" OHSR at "+Date+": "+str(OHSR)+", "
                    if Availability!="None)":
                        if (Availability<Availability_2G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" Availability at "+Date+": "+str(Availability)+", "
                    else:
                        NotUpdated_Availability_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" Availability at "+Date+": "+str(Availability)+", "

                    Cell_Count+=1
                    if (Status=="Not Updated" and (NotUpdated_TCH_Traffic_Count==Cell_Count or NotUpdated_CSSR_Count==Cell_Count or NotUpdated_CDR_Count==Cell_Count or NotUpdated_IHSR_Count==Cell_Count or NotUpdated_OHSR_Count==Cell_Count or NotUpdated_Availability_Count==Cell_Count)):
                        Status='Not Updated'
                    else:
                       if Status!="Reject":
                           Status='Pass'
               
                    if Rejecteded_List!="":
                        Status="Reject"
                    KPI0_2G_Results[i+1][0] =ID
                    KPI0_2G_Results[i+1][1] =Site
                    KPI0_2G_Results[i+1][2] =Status
                    KPI0_2G_Results[i+1][3] =Currect_date_day
                    KPI0_2G_Results[i+1][4] =Rejecteded_List
                    KPI0_2G_Results[i+1][5] =NotUpdated_List
            if (Cell_Count==0):
                KPI0_2G_Results[i+1][0] =ID
                KPI0_2G_Results[i+1][1] =Site
                KPI0_2G_Results[i+1][2] ="No Data"
                KPI0_2G_Results[i+1][3] =Currect_date_day
                KPI0_2G_Results[i+1][4] =""
                KPI0_2G_Results[i+1][5] =""



    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()



    for n in range(len(KPI0_2G_Results)-1):
        Update_String_Quary="UPDATE CM SET kpiZDate = '"+KPI0_2G_Results[n+1][3]+"' , kpiZStatus ='"+KPI0_2G_Results[n+1][2]+"' WHERE id="+ str(KPI0_2G_Results[n+1][0])+ " and Site='"+KPI0_2G_Results[n+1][1]+"'"
        conn_apache_Desk.execute(Update_String_Quary)
        conn_apache_Desk.commit()


    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()


    for n in range(len(KPI0_2G_Results)-1):
        Update_String_Quary="UPDATE CM SET CMStatus = 4  , pendingTeam='SSV' WHERE  kpiZStatus = 'Pass' and id="+ str(KPI0_2G_Results[n+1][0])+ " and Site='"+KPI0_2G_Results[n+1][1]+"'"
        conn_apache_Desk.execute(Update_String_Quary)
        conn_apache_Desk.commit()

    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()


    for n in range(len(KPI0_2G_Results)-1):
        CMID=KPI0_2G_Results[n+1][0]
        userID=0
        mail="spring@nak-mci.ir"
        if KPI0_2G_Results[n+1][2]=="Pass":
            activityType="pass"
        else:
            activityType=""
        activityDate=datetime.datetime.now()
        site=KPI0_2G_Results[n+1][1]
        location=site[0:3]+Site[4:8]
        status="KPI0 "+KPI0_2G_Results[n+1][2]
        conn_apache_Desk.execute('''INSERT INTO CM_Activity (CMID,userID,mail,activityType,activityDate,siteId,locationId,action) VALUES (?,?,?,?,?,?,?,?) ''',
        CMID,
        userID,
        mail,
        activityType,
        activityDate,
        site,
        location,
        status
        )
        conn_apache_Desk.commit()

    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()

    for n in range(len(KPI0_2G_Results)-1):
        conn_apache_Desk.execute('''INSERT INTO KPIZeroStatus (CMID,Site,KPIZDate,KPIZStatus,Rejected_List,Non_Updated_List) VALUES (?,?,?,?,?,?) ''',
        KPI0_2G_Results[n+1][0],
        KPI0_2G_Results[n+1][1],
        KPI0_2G_Results[n+1][3],
        KPI0_2G_Results[n+1][2],
        KPI0_2G_Results[n+1][4],
        KPI0_2G_Results[n+1][5]
        )
        conn_apache_Desk.commit()







# ******************************** 3G *********************************

# ----------------------------- 3G CM Table -------------------------------

if Run_3G:
    conn_apache_Desk.execute("select  id, province, location, site, tech, bscRnc,  vendor, [integrationStatus], [rssvDate], CMStatus , [kpiZDate],  [kpiZStatus] from CM where [integrationStatus]='Fully Integrated' and ([kpiZStatus]!='Pass') and (substring(tech,2,1)='U')"+
    " union all "+
    "select  id, province, location, site, tech, bscRnc,  vendor, [integrationStatus], [rssvDate], CMStatus , [kpiZDate],  [kpiZStatus] from CM where [integrationStatus]='Fully Integrated' and ([kpiZStatus] is null) and (substring(tech,2,1)='U')" 
    )
    CM_3G_Table=conn_apache_Desk.fetchall()


    Ericsson_3G_list=""
    Huawei_3G_list=""
    Nokia_3G_list=""


    for i in range(len(CM_3G_Table)):


        Row_3G=str(CM_3G_Table[i])
        Row_3G=Row_3G.split(", ")

        # Finding column number of headers 
        location_header_ind=0
        site_header_ind=0 
        tech_header_ind=0
        vendor_header_ind=0
        for k in range(len(CM_3G_Table[i].cursor_description)):
            hearder_string=str(CM_3G_Table[i].cursor_description[k])
            if hearder_string[2:10]=="location":
                location_header_ind=k
            if hearder_string[2:6]=="site":
                site_header_ind=k
            if hearder_string[2:6]=="tech":
                tech_header_ind=k
            if hearder_string[2:8]=="vendor":
                vendor_header_ind=k
        ID=Row_3G[0]
        ID=int(ID[1:len(ID)])
        if location_header_ind==0:
            Location=""
        else:
            Location=Row_3G[location_header_ind]
        if site_header_ind==0:
            Site=""
        else:
            Site=Row_3G[site_header_ind]
        if tech_header_ind==0:
            Band=""
        else:
            Band=Row_3G[tech_header_ind]
        if vendor_header_ind==0:
            Vendor=""
        else:
            Vendor=Row_3G[vendor_header_ind]
        Site=Site[1:len(Site)-1]

        if Vendor=="'Ericsson'":
            Ericsson_Site=Site
            Ericsson_3G_list=Ericsson_3G_list+"substring([ElementID1],1,8)='" + Ericsson_Site + "' or ";
        if Vendor=="'Nokia'":
            Nokia_Site=Site
            Nokia_3G_list=Nokia_3G_list+"substring([ElementID1],1,8)='" + Nokia_Site + "' or ";
        if Vendor=="'Huawei'":
            Huawei_Site=Site
            Huawei_3G_list=Huawei_3G_list+"substring([ElementID1],1,8)='" + Huawei_Site + "' or ";



    Ericsson_3G_list=Ericsson_3G_list[0:len(Ericsson_3G_list)-4];
    Huawei_3G_list=Huawei_3G_list[0:len(Huawei_3G_list)-4];
    Nokia_3G_list=Nokia_3G_list[0:len(Nokia_3G_list)-4];





    # ------------------------------ 3G KPI Tables -------------------------------

    if Ericsson_3G_list!="":
        conn_performanceDB.execute("select Day, RNC, Site, Sector, sum([CS_Traffic_BH (Erlang)]) as 'CS_Traffic_BH (Erlang)', avg([CS RAB Establish]) as 'CS RAB Establish', avg([CS RRC SR]) as 'CS RRC SR', avg([Voice Drop Rate]) as 'Voice Drop Rate', avg([Cell Availability]) as 'Cell Availability' from (select [Date], substring(convert(varchar,Date,23),1,10) as 'Day', [ElementID] as 'RNC', 'Ericsson' as 'Vendor', substring([ElementID1],1,8) as 'Site', substring([ElementID1],1,9) as 'Sector', [ElementID1] as 'Cell', [CS_Traffic_BH] as 'CS_Traffic_BH (Erlang)', [Cs_RAB_Establish_Success_Rate] as 'CS RAB Establish', [CS_RRC_Setup_Success_Rate] as'CS RRC SR', [CS_Drop_Call_Rate] as 'Voice Drop Rate', [Cell_Availability_Rate_Exclude_Blocking(UCELL_Eric)] as 'Cell Availability' from [dbo].[CC3_Ericsson_Cell_BH] where  (" + Ericsson_3G_list + ") and (" + date_list1 + ")) tble group by Day, RNC, Site, Sector")
        KPI_Ericsson_3G_CS_Table = conn_performanceDB.fetchall()
        conn_performanceDB.execute("select Day, RNC, Site, Sector, sum([PS_Traffic_Daily (GB)]) as 'PS_Traffic_Daily (GB)', avg([PS RRC SR]) as 'PS RRC SR', avg([PS Drop Rate]) as 'PS Drop Rate', avg([RSSI]) as 'RSSI', avg(THR) as 'THR' from (select [Date], substring(convert(varchar,Date,23),1,10) as 'Day', [ElementID] as 'RNC', 'Ericsson' as 'Vendor', substring([ElementID1],1,8) as 'Site', substring([ElementID1],1,9) as 'Sector', [ElementID1] as 'Cell', [PS_Volume(GB)(UCell_Eric)] as 'PS_Traffic_Daily (GB)', [PS_RRC_Setup_Success_Rate(UCell_Eric)] as 'PS RRC SR', [PS_Drop_Call_Rate(UCell_Eric)] as'PS Drop Rate', [uplink_average_RSSI_dbm_(Eric_UCELL)] as 'RSSI', [HS_USER_Throughput_NET_PQ(Mbps)(UCell_Eric)] as 'THR' from [dbo].[RD3_Ericsson_Cell_Daily] where  (" + Ericsson_3G_list + ") and (" + date_list1 + ")) tble group by Day, RNC, Site, Sector")   
        KPI_Ericsson_3G_PS_Table = conn_performanceDB.fetchall()
    if Huawei_3G_list!="":
        conn_performanceDB.execute("select Day, RNC, Site, Sector, sum([CS_Erlang (Erlang)]) as 'CS_Erlang (Erlang)', avg([CS RAB Establish]) as 'CS RAB Establish', avg([CS RRC SR]) as 'CS RRC SR', avg([Voice Drop Rate]) as 'Voice Drop Rate', avg([Cell Availability]) as 'Cell Availability' from (select [Date], substring(convert(varchar,Date,23),1,10) as 'Day', [ElementID] as 'RNC', 'Huawei' as 'Vendor', substring([ElementID1],1,8) as 'Site', substring([ElementID1],1,9) as 'Sector', [ElementID1] as 'Cell', [CS_Erlang] as 'CS_Erlang (Erlang)', [CS_RAB_Setup_Success_Ratio] as 'CS RAB Establish', [CS_RRC_Connection_Establishment_SR] as'CS RRC SR', [AMR_Call_Drop_Ratio_New(Hu_CELL)] as 'Voice Drop Rate', [Radio_Network_Availability_Ratio(Hu_Cell)] as 'Cell Availability' from [dbo].[CC3_Huawei_Cell_BH] where  (" + Huawei_3G_list + ") and (" + date_list1 + ")) tble group by Day, RNC, Site, Sector")
        KPI_Huawei_3G_CS_Table = conn_performanceDB.fetchall()
        conn_performanceDB.execute("select Day, RNC, Site, Sector, sum([PS_Traffic_Daily (GB)]) as 'PS_Traffic_Daily (GB)', avg([PS RRC SR]) as 'PS RRC SR', avg([PS Drop Rate]) as 'PS Drop Rate', avg([RSSI]) as 'RSSI', avg(THR) as 'THR' from (select [Date], substring(convert(varchar,Date,23),1,10) as 'Day', [ElementID] as 'RNC', 'Huawei' as 'Vendor', substring([ElementID1],1,8) as 'Site', substring([ElementID1],1,9) as 'Sector', [ElementID1] as 'Cell', [PAYLOAD] as 'PS_Traffic_Daily (GB)', [PS_RRC_Connection_success_Rate_repeatless(Hu_Cell)] as 'PS RRC SR', [PS_Call_Drop_Ratio] as'PS Drop Rate', [Mean_RTWP(Cell_Hu)] as 'RSSI', [AVERAGE_HSDPA_USER_THROUGHPUT_DC+SC(Mbit/s)(CELL_HUAWEI)] as 'THR' from [dbo].[RD3_Huawei_Cell_Daily] where  (" + Huawei_3G_list + ") and (" + date_list1 + ")) tble group by Day, RNC, Site, Sector")   
        KPI_Huawei_3G_PS_Table = conn_performanceDB.fetchall()
    if Nokia_3G_list!="":
        conn_performanceDB.execute("select Day, RNC, Site, Sector, sum([CS_Traffic (Erlang)]) as 'CS_Traffic (Erlang)', avg([CS RAB Establish]) as 'CS RAB Establish', avg([CS RRC SR]) as 'CS RRC SR', avg([Voice Drop Rate]) as 'Voice Drop Rate', avg([Cell Availability]) as 'Cell Availability' from (select [Date], substring(convert(varchar,Date,23),1,10) as 'Day', [ElementID] as 'RNC', 'Nokia' as 'Vendor', substring([ElementID1],1,8) as 'Site', substring([ElementID1],1,9) as 'Sector', [ElementID1] as 'Cell', [CS_TrafficBH] as 'CS_Traffic (Erlang)', [CS_RAB_Establish_Success_Rate] as 'CS RAB Establish', [CS_RRC_SETUP_SR_WITHOUT_REPEAT(CELL_NOKIA)] as'CS RRC SR', [CS_Drop_Call_Rate] as 'Voice Drop Rate', [Cell_Availability_excluding_blocked_by_user_state] as 'Cell Availability' from [dbo].[CC3_Nokia_Cell_BH] where  (" + Nokia_3G_list + ") and (" + date_list1 + ")) tble group by Day, RNC, Site, Sector")
        KPI_Nokia_3G_CS_Table = conn_performanceDB.fetchall()
        conn_performanceDB.execute("select Day, RNC, Site, Sector, sum([PS_Traffic_Daily (GB)]) as 'PS_Traffic_Daily (GB)', avg([PS RRC SR]) as 'PS RRC SR', avg([PS Drop Rate]) as 'PS Drop Rate', avg([RSSI]) as 'RSSI' from (select [Date], substring(convert(varchar,Date,23),1,10) as 'Day', [ElementID] as 'RNC', 'Nokia' as 'Vendor', substring([ElementID1],1,8) as 'Site', substring([ElementID1],1,9) as 'Sector', [ElementID1] as 'Cell', [PS_Payload_Total(HS+R99)(Nokia_CELL)_GB] as 'PS_Traffic_Daily (GB)', [PS_RRCSETUP_SR] as 'PS RRC SR', [Packet_Session_Drop_Ratio_NOKIA(CELL_NOKIA)] as'PS Drop Rate', [average_RTWP_dbm(Nokia_Cell)] as 'RSSI' from [dbo].[RD3_Nokia_Cell_Daily] where  (" + Nokia_3G_list + ") and (" + date_list1 + ")) tble group by Day, RNC, Site, Sector")   
        KPI_Nokia_3G_PS_Table = conn_performanceDB.fetchall()





    KPI0_3G_Results=[]
    n = len(CM_3G_Table)+1
    m = 6
    KPI0_3G_Results = [""] * n
    for x in range (n):
        KPI0_3G_Results[x] = [""] * m

    KPI0_3G_Results[0][0] ='ID'
    KPI0_3G_Results[0][1] ='Site'
    KPI0_3G_Results[0][2] ='Statue'
    KPI0_3G_Results[0][3] ='Update_Date'
    KPI0_3G_Results[0][4] ='Rejecteded_List'
    KPI0_3G_Results[0][5] ='NotUpdated_List'


    for i in range(len(CM_3G_Table)):
        Row_3G=str(CM_3G_Table[i])
        Row_3G=Row_3G.split(", ")

        # Finding column number of headers 
        site_header_ind=0 
        for k in range(len(CM_3G_Table[i].cursor_description)):
            hearder_string=str(CM_3G_Table[i].cursor_description[k])
            if hearder_string[2:10]=="location":
                location_header_ind=k
            if hearder_string[2:6]=="site":
                site_header_ind=k
            if hearder_string[2:6]=="tech":
                tech_header_ind=k
            if hearder_string[2:8]=="vendor":
                vendor_header_ind=k
        ID=Row_3G[0]
        ID=int(ID[1:len(ID)])
        if location_header_ind==0:
            Location=""
        else:
            location=Row_3G[location_header_ind]
            location=location[1:len(location)-1]
        if site_header_ind==0:
            Site=""
        else:
            Site=Row_3G[site_header_ind]
            Site=Site[1:len(Site)-1]
        if tech_header_ind==0:
            Band=""
        else:
            Band=Row_3G[tech_header_ind]
        if vendor_header_ind==0:
            Vendor=""
        else:
            Vendor=Row_3G[vendor_header_ind]

        KPI_Table_PS=[]
        KPI_Table_CS=[]
        if Vendor=="'Nokia'":
            KPI_Table_CS=KPI_Nokia_3G_CS_Table
            KPI_Table_PS=KPI_Nokia_3G_PS_Table 
        if Vendor=="'Huawei'":
            KPI_Table_CS=KPI_Huawei_3G_CS_Table
            KPI_Table_PS=KPI_Huawei_3G_PS_Table 
        if Vendor=="'Ericsson'":
            KPI_Table_CS=KPI_Ericsson_3G_CS_Table
            KPI_Table_PS=KPI_Ericsson_3G_PS_Table 


        Status="Pass"
        Rejecteded_List=""
        Not_Updated_List=""
        NotUpdated_List=""

        NotUpdated_CS_Traffic_Count=0
        NotUpdated_CS_RAB_Count=0
        NotUpdated_CS_RRC_Count=0
        NotUpdated_CS_Drop_Count=0
        NotUpdated_Availability_Count=0
        NotUpdated_PS_Traffic_Count=0
        NotUpdated_PS_RRC_Count=0
        NotUpdated_PS_Drop_Count=0
        NotUpdated_RSSI_Count=0
        NotUpdated_THR_Count=0


        Cell_Count_CS=0
        Cell_Count_PS=0

        KPI0_3G_Results[i+1][0] =ID
        KPI0_3G_Results[i+1][1] =Site
        KPI0_3G_Results[i+1][2] =Status
        KPI0_3G_Results[i+1][3] =Currect_date_day
        KPI0_3G_Results[i+1][4] =Rejecteded_List
        KPI0_3G_Results[i+1][5] =NotUpdated_List



        if len(KPI_Table_CS)==0 and len(KPI_Table_PS)==0:
            KPI0_3G_Results[i+1][0] =ID
            KPI0_3G_Results[i+1][1] =Site
            KPI0_3G_Results[i+1][2] ="No Data"
            KPI0_3G_Results[i+1][3] =Currect_date_day
            KPI0_3G_Results[i+1][4] =""
            KPI0_3G_Results[i+1][5] =""
        if len(KPI_Table_CS)!=0:
            for p in range(len(KPI_Table_CS)):
                Row_3G_KPI=str(KPI_Table_CS[p])
                Row_3G_KPI=Row_3G_KPI.split(", ")
                Candidate_Site=Row_3G_KPI[2]
                Candidate_Site=Candidate_Site[1:len(Candidate_Site)-1]


                if Candidate_Site==Site:
                    Date=Row_3G_KPI[0]
                    Date=Date[2:len(Date)-1]
                    Cell=Row_3G_KPI[3]
                    Cell=Cell[1:len(Cell)-1]
                    CS_Traffic=Row_3G_KPI[4]
                    CS_RAB=Row_3G_KPI[5]
                    CS_RRC=Row_3G_KPI[6]
                    Voice_Drop=Row_3G_KPI[7]
                    Availability=Row_3G_KPI[8]
                    Availability=Availability[0:len(Availability)-1]

                    if (CS_Traffic!="None"):
                        CS_Traffic=float("{:.2f}".format(float(CS_Traffic)))
                    if (CS_RAB!="None"):
                        CS_RAB=float("{:.2f}".format(float(CS_RAB)))
                    if (CS_RRC!="None"):
                        CS_RRC=float("{:.2f}".format(float(CS_RRC)))
                    if (Voice_Drop!="None"):
                        Voice_Drop=float("{:.2f}".format(float(Voice_Drop)))
                    if (Availability!="None"):
                        Availability=float("{:.2f}".format(float(Availability)))


                    if CS_Traffic!="None":
                        if (CS_Traffic<=CS_Traffic_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CS_Traffic at "+Date+": "+str(CS_Traffic)+", "
                    else:
                        NotUpdated_CS_Traffic_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" CS_Traffic at "+Date+": "+str(CS_Traffic)+", "
                    if CS_RAB!="None":
                        if (CS_RAB<CS_RAB_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CS_RAB at "+Date+": "+str(CS_RAB)+", "
                    else:
                        NotUpdated_CS_RAB_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" CS_RAB at "+Date+": "+str(CS_RAB)+", "
                    if CS_RRC!="None":
                        if (CS_RRC<CS_RRC_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CS_RRC at "+Date+": "+str(CS_RRC)+", "
                    else:
                        NotUpdated_CS_RRC_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" CS_RRC at "+Date+": "+str(CS_RRC)+", "
                    if Voice_Drop!="None":
                        if (Voice_Drop>CS_Drop_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" Voice_Drop at "+Date+": "+str(Voice_Drop)+", "
                    else:
                        NotUpdated_CS_Drop_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" Voice_Drop at "+Date+": "+str(Voice_Drop)+", "
                    if Availability!="None":
                        if (Availability<Availability_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" Availability at "+Date+": "+str(Availability)+", "
                    else:
                        NotUpdated_Availability_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" Availability at "+Date+": "+str(Availability)+", "


                    Cell_Count_CS+=1
                    if (Status=="Not Updated" and (NotUpdated_CS_Traffic_Count==Cell_Count_CS or NotUpdated_CS_RAB_Count==Cell_Count_CS or NotUpdated_CS_RRC_Count==Cell_Count_CS or NotUpdated_CS_Drop_Count==Cell_Count_CS or NotUpdated_Availability_Count==Cell_Count_CS)):
                        Status='Not Updated'
                    else:
                       if Status!="Reject":
                           Status='Pass'

                    if Rejecteded_List!="":
                        Status="Reject"
                    KPI0_3G_Results[i+1][0] =ID
                    KPI0_3G_Results[i+1][1] =Site
                    KPI0_3G_Results[i+1][2] =Status
                    KPI0_3G_Results[i+1][3] =Currect_date_day
                    KPI0_3G_Results[i+1][4] =Rejecteded_List
                    KPI0_3G_Results[i+1][5] =NotUpdated_List

            if (Cell_Count_CS==0):
                KPI0_3G_Results[i+1][0] =ID
                KPI0_3G_Results[i+1][1] =Site
                KPI0_3G_Results[i+1][2] ="No Data"
                KPI0_3G_Results[i+1][3] =Currect_date_day
                KPI0_3G_Results[i+1][4] =""
                KPI0_3G_Results[i+1][5] =""

        if len(KPI_Table_PS)!=0:
            for p in range(len(KPI_Table_PS)):
                Row_3G_KPI=str(KPI_Table_PS[p])
                Row_3G_KPI=Row_3G_KPI.split(", ")
                Candidate_Site=Row_3G_KPI[2]
                Candidate_Site=Candidate_Site[1:len(Candidate_Site)-1]


                if Candidate_Site==Site:
                    Date=Row_3G_KPI[0]
                    Date=Date[2:len(Date)-1]
                    Cell=Row_3G_KPI[3]
                    Cell=Cell[1:len(Cell)-1]
                    PS_Traffic=Row_3G_KPI[4]
                    PS_RRC=Row_3G_KPI[5]
                    PS_Drop=Row_3G_KPI[6]
                    if Vendor=="'Nokia'":
                        RSSI=Row_3G_KPI[7]
                        RSSI=RSSI[0:len(RSSI)-1]
                    if Vendor!="'Nokia'":
                        RSSI=Row_3G_KPI[7]
                        THR=Row_3G_KPI[8]
                        THR=THR[0:len(THR)-1]

                    if (PS_Traffic!="None"):
                        PS_Traffic=float("{:.2f}".format(float(PS_Traffic)))
                    if (PS_RRC!="None"):
                        PS_RRC=float("{:.2f}".format(float(PS_RRC)))
                    if (PS_Drop!="None"):
                        PS_Drop=float("{:.2f}".format(float(PS_Drop)))
                    if (RSSI!="None"):
                        RSSI=float("{:.2f}".format(float(RSSI)))
                    if Vendor!="'Nokia'":
                        if THR!="None":
                            THR=float(THR)

                    if PS_Traffic!="None":
                        if (PS_Traffic<=PS_Traffic_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" PS_Traffic at "+Date+": "+str(PS_Traffic)+", "
                    else:
                        NotUpdated_PS_Traffic_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" PS_Traffic at "+Date+": "+str(PS_Traffic)+", "
                    if PS_RRC!="None":
                        if (PS_RRC<PS_RRC_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" PS_RRC at "+Date+": "+str(PS_RRC)+", "
                    else:
                        NotUpdated_PS_RRC_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" PS_RRC at "+Date+": "+str(PS_RRC)+", "
                    if PS_Drop!="None":
                        if (PS_Drop>PS_Drop_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" PS_Drop at "+Date+": "+str(PS_Drop)+", "
                    else:
                        NotUpdated_PS_Drop_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" PS_Drop at "+Date+": "+str(PS_Drop)+", "
                    if RSSI!="None":
                        if (RSSI>RSSI_3G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" RSSI at "+Date+": "+str(RSSI)+", "
                    else:
                        NotUpdated_RSSI_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" RSSI at "+Date+": "+str(RSSI)+", "
                    if Vendor!="'Nokia'":
                        if THR!="None":
                            if (THR<THR_3G):
                                Status='Reject'
                                Rejecteded_List=Rejecteded_List+Cell+" THR at "+Date+": "+str(THR)+", "
                        else:
                            NotUpdated_THR_Count+=1
                            Status='Not Updated'
                            NotUpdated_List=NotUpdated_List+Cell+" THR at "+Date+": "+str(THR)+", "
 


                    Cell_Count_PS+=1
                    if (Status=="Not Updated" and (NotUpdated_PS_Traffic_Count==Cell_Count_PS or NotUpdated_PS_RRC_Count==Cell_Count_PS or NotUpdated_PS_Drop_Count==Cell_Count_PS or NotUpdated_RSSI_Count==Cell_Count_PS or NotUpdated_THR_Count==Cell_Count_PS)):
                        Status='Not Updated'
                    else:
                       if Status!="Reject":
                           Status='Pass'

                    if Rejecteded_List!="":
                        Status="Reject"
                    KPI0_3G_Results[i+1][0] =ID
                    KPI0_3G_Results[i+1][1] =Site
                    KPI0_3G_Results[i+1][2] =Status
                    KPI0_3G_Results[i+1][3] =Currect_date_day
                    KPI0_3G_Results[i+1][4] =Rejecteded_List
                    KPI0_3G_Results[i+1][5] =NotUpdated_List

            if (Cell_Count_PS==0):
                KPI0_3G_Results[i+1][0] =ID
                KPI0_3G_Results[i+1][1] =Site
                KPI0_3G_Results[i+1][2] ="No Data"
                KPI0_3G_Results[i+1][3] =Currect_date_day
                KPI0_3G_Results[i+1][4] =""
                KPI0_3G_Results[i+1][5] =""


    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()



    for n in range(len(KPI0_3G_Results)-1):
        Update_String_Quary="UPDATE CM SET kpiZDate = '"+KPI0_3G_Results[n+1][3]+"' , kpiZStatus ='"+KPI0_3G_Results[n+1][2]+"' WHERE id="+ str(KPI0_3G_Results[n+1][0])+ " and Site='"+KPI0_3G_Results[n+1][1]+"'"
        conn_apache_Desk.execute(Update_String_Quary)
        conn_apache_Desk.commit()



    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()




    for n in range(len(KPI0_3G_Results)-1):
        Update_String_Quary="UPDATE CM SET CMStatus = 4  , pendingTeam='SSV' WHERE  kpiZStatus = 'Pass' and id="+ str(KPI0_3G_Results[n+1][0])+ " and Site='"+KPI0_3G_Results[n+1][1]+"'"
        conn_apache_Desk.execute(Update_String_Quary)
        conn_apache_Desk.commit()


    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()




    for n in range(len(KPI0_3G_Results)-1):
        CMID=KPI0_3G_Results[n+1][0]
        userID=0
        mail="spring@nak-mci.ir"
        if KPI0_3G_Results[n+1][2]=="Pass":
            activityType="pass"
        else:
            activityType=""
        activityDate=datetime.datetime.now()
        site=KPI0_3G_Results[n+1][1]
        location=site[0:3]+Site[4:8]
        status="KPIZeo "+KPI0_3G_Results[n+1][2]
        conn_apache_Desk.execute('''INSERT INTO CM_Activity (CMID,userID,mail,activityType,activityDate,siteId,locationId,action) VALUES (?,?,?,?,?,?,?,?) ''',
        CMID,
        userID,
        mail,
        activityType,
        activityDate,
        site,
        location,
        status
        )
        conn_apache_Desk.commit()

    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()


    for n in range(len(KPI0_3G_Results)-1):
        conn_apache_Desk.execute('''INSERT INTO KPIZeroStatus (CMID,Site,KPIZDate,KPIZStatus,Rejected_List,Non_Updated_List) VALUES (?,?,?,?,?,?) ''',
        KPI0_3G_Results[n+1][0],
        KPI0_3G_Results[n+1][1],
        KPI0_3G_Results[n+1][3],
        KPI0_3G_Results[n+1][2],
        KPI0_3G_Results[n+1][4],
        KPI0_3G_Results[n+1][5]
        )
        conn_apache_Desk.commit()











## ******************************** 4G *********************************

## ----------------------------- 4G CM Table -------------------------------

if Run_4G:
    conn_apache_Desk.execute("select id, province, location, site, tech, bscRnc,  vendor, [integrationStatus], [rssvDate], CMStatus , [kpiZDate],  [kpiZStatus] from CM where [integrationStatus]='Fully Integrated' and ([kpiZStatus]!='Pass') and (substring(tech,1,2)='1L' or substring(tech,1,2)='2L' or substring(tech,1,2)='3L' or substring(tech,1,2)='4L' or substring(tech,1,2)='5L' or substring(tech,1,2)='6L' or substring(tech,1,2)='7L' or substring(tech,1,2)='8L' or substring(tech,1,2)='9L' or substring(tech,1,2)='AL' or substring(tech,1,2)='BL')"+
    " union all "+
    "select id, province, location, site, tech, bscRnc,  vendor, [integrationStatus], [rssvDate], CMStatus , [kpiZDate],  [kpiZStatus] from CM where [integrationStatus]='Fully Integrated' and ([kpiZStatus] is null) and (substring(tech,1,2)='1L' or substring(tech,1,2)='2L' or substring(tech,1,2)='3L' or substring(tech,1,2)='4L' or substring(tech,1,2)='5L' or substring(tech,1,2)='6L' or substring(tech,1,2)='7L' or substring(tech,1,2)='8L' or substring(tech,1,2)='9L' or substring(tech,1,2)='AL' or substring(tech,1,2)='BL')"
    )
    CM_4G_Table=conn_apache_Desk.fetchall()


    Ericsson_4G_list=""
    Huawei_4G_list=""
    Nokia_4G_list=""
    Ericsson_Volte_list=""
    Huawei_Volte_list=""
    Nokia_Volte_list=""


    for i in range(len(CM_4G_Table)):


        Row_4G=str(CM_4G_Table[i])
        Row_4G=Row_4G.split(", ")

        # Finding column number of headers 
        location_header_ind=0
        site_header_ind=0 
        tech_header_ind=0
        vendor_header_ind=0
        for k in range(len(CM_4G_Table[i].cursor_description)):
            hearder_string=str(CM_4G_Table[i].cursor_description[k])
            if hearder_string[2:10]=="location":
                location_header_ind=k
            if hearder_string[2:6]=="site":
                site_header_ind=k
            if hearder_string[2:6]=="tech":
                tech_header_ind=k
            if hearder_string[2:8]=="vendor":
                vendor_header_ind=k
        ID=Row_4G[0]
        ID=int(ID[1:len(ID)])
        if location_header_ind==0:
            Location=""
        else:
            Location=Row_4G[location_header_ind]
        if site_header_ind==0:
            Site=""
        else:
            Site=Row_4G[site_header_ind]
        if tech_header_ind==0:
            Band=""
        else:
            Band=Row_4G[tech_header_ind]
        if vendor_header_ind==0:
            Vendor=""
        else:
            Vendor=Row_4G[vendor_header_ind]
        Site=Site[1:len(Site)-1]

        if Vendor=="'Ericsson'":
            Ericsson_Site=Site
            Ericsson_4G_list=Ericsson_4G_list+"substring([eNodeB],1,8)='" + Ericsson_Site + "' or ";
            Ericsson_Volte_list=Ericsson_Volte_list+"substring([Cell_Name],1,8)='" + Ericsson_Site + "' or ";
        if Vendor=="'Nokia'":
            Nokia_Site=Site
            Nokia_4G_list=Nokia_4G_list+"substring([ElementID1],1,8)='" + Nokia_Site + "' or ";
            Nokia_Volte_list=Nokia_Volte_list+"substring([Cell_Name],1,8)='" + Nokia_Site + "' or ";
        if Vendor=="'Huawei'":
            Huawei_Site=Site
            Huawei_4G_list=Huawei_4G_list+"substring([eNodeB],1,8)='" + Huawei_Site + "' or ";
            Huawei_Volte_list=Huawei_Volte_list+"substring([Cell_Name],1,8)='" + Huawei_Site + "' or ";


    Ericsson_4G_list=Ericsson_4G_list[0:len(Ericsson_4G_list)-4];
    Huawei_4G_list=Huawei_4G_list[0:len(Huawei_4G_list)-4];
    Nokia_4G_list=Nokia_4G_list[0:len(Nokia_4G_list)-4];
    Ericsson_Volte_list=Ericsson_Volte_list[0:len(Ericsson_Volte_list)-4];
    Huawei_Volte_list=Huawei_Volte_list[0:len(Huawei_Volte_list)-4];
    Nokia_Volte_list=Nokia_Volte_list[0:len(Nokia_Volte_list)-4];


    # ------------------------------ 4G KPI Tables -------------------------------

    if Ericsson_4G_list!="":
        conn_performanceDB.execute("select Day, Site, Sector, sum([PS_Traffic_Daily (GB)]) as 'PS_Traffic_Daily (GB)', avg([UE DL THR (Mbps)]) as 'UE DL THR (Mbps)',  avg([UE UL THR (Mbps)]) as 'UE UL THR (Mbps)', avg([ERAB Drop Rate]) as 'ERAB Drop Rate', avg([ERAB Setup SR]) as 'ERAB Setup SR', avg([Intra Freq HO SR]) as 'Intra Freq HO SR', avg([RRC Connection SR]) as 'RRC Connection SR' , avg([Cell Availability]) as 'Cell Availability' from (select [Datetime], substring(convert(varchar,Datetime,23),1,10) as 'Day', substring([eNodeB],1,8) as 'Site', substring([eNodeB],1,9) as 'Sector', [eNodeB] as 'Cell', [Total_Volume(UL+DL)(GB)(eNodeB_Eric)] as 'PS_Traffic_Daily (GB)', [Average_UE_DL_Throughput(Mbps)(eNodeB_Eric)] as 'UE DL THR (Mbps)', [Average_UE_UL_Throughput(Mbps)(eNodeB_Eric)] as'UE UL THR (Mbps)', [E_RAB_Drop_Rate(eNodeB_Eric)] as 'ERAB Drop Rate', [E-RAB_Setup_SR_incl_added_New(EUCell_Eric)] as 'ERAB Setup SR', [IntraF_Handover_Execution(eNodeB_Eric)] as 'Intra Freq HO SR' , [RRC_Estab_Success_Rate(ReAtt)(EUCell_Eric)] as 'RRC Connection SR' , [Cell_Availability_Rate_Exclude_Blocking(Cell_EricLTE)] as 'Cell Availability' from [dbo].[TBL_LTE_CELL_Daily_E] where  (" + Ericsson_4G_list + ") and (" + date_list1_lte + ")) tble group by Day,  Site, Sector")
        KPI_Ericsson_4G_Table = conn_performanceDB.fetchall()
        conn_apache_Spring.execute("select Date_Key, Site, Sector, sum(cast([VoLTE_Traffic_Erlang] as float)) as 'Volte' from (select Date_Key, substring(Cell_Name,1,8) as 'Site', substring(Cell_Name,1,9) as 'Sector',  [VoLTE_Traffic_Erlang] from [Volte_CSFB] where  [VoLTE_Traffic_Erlang] is not null and  (" + Ericsson_Volte_list + ") and (" + date_list1_Nokia_G1800 + ")) tble group by Date_Key, Site, Sector")
        Volte_Ericsson_4G_Table = conn_apache_Spring.fetchall()
        conn_apache_Spring.execute("select Date_Key, Site, Sector, avg(cast([CSFB_Success_Rate] as float)) as 'CSFB' from ( select Date_Key, substring(Cell_Name,1,8) as 'Site', substring(Cell_Name,1,9) as 'Sector',  [CSFB_Success_Rate] from [Volte_CSFB] where  [CSFB_Success_Rate] is not null and  (" + Ericsson_Volte_list + ") and (" + date_list1_Nokia_G1800 + ")) tble group by Date_Key, Site, Sector")
        CSFB_Ericsson_4G_Table = conn_apache_Spring.fetchall()

    if Huawei_4G_list!="":
        conn_performanceDB.execute("select Day, Site, Sector, sum([PS_Traffic_Daily (GB)]) as 'PS_Traffic_Daily (GB)', avg([UE DL THR (Mbps)]) as 'UE DL THR (Mbps)',  avg([UE UL THR (Mbps)]) as 'UE UL THR (Mbps)', avg([ERAB Drop Rate]) as 'ERAB Drop Rate', avg([ERAB Setup SR]) as 'ERAB Setup SR', avg([Intra Freq HO SR]) as 'Intra Freq HO SR', avg([RRC Connection SR]) as 'RRC Connection SR' , avg([Cell Availability]) as 'Cell Availability'  from (select [Datetime], substring(convert(varchar,Datetime,23),1,10) as 'Day', substring([eNodeB],1,8) as 'Site', substring([eNodeB],1,9) as 'Sector', [eNodeB] as 'Cell',  [Total_Traffic_Volume(GB)] as 'PS_Traffic_Daily (GB)', [Average_Downlink_User_Throughput(Mbit/s)] as 'UE DL THR (Mbps)', [Average_UPlink_User_Throughput(Mbit/s)] as'UE UL THR (Mbps)',  [Call_Drop_Rate] as 'ERAB Drop Rate',  [E-RAB_Setup_Success_Rate(Hu_Cell)] as 'ERAB Setup SR'  , [IntraF_HOOut_SR] as 'Intra Freq HO SR' , [RRC_Connection_Setup_Success_Rate_service] as 'RRC Connection SR' , [Cell_Availability_Rate_Exclude_Blocking(Cell_Hu)] as 'Cell Availability' from [dbo].[TBL_LTE_CELL_Daily_H] where  (" + Huawei_4G_list + ") and (" + date_list1_lte + ")) tble group by Day, Site, Sector")
        KPI_Huawei_4G_Table = conn_performanceDB.fetchall()
        conn_apache_Spring.execute("select Date_Key, Site, Sector, sum(cast([VoLTE_Traffic_Erlang] as float)) as 'Volte' from (select Date_Key, substring(Cell_Name,1,8) as 'Site', substring(Cell_Name,1,9) as 'Sector',  [VoLTE_Traffic_Erlang] from [Volte_CSFB] where  [VoLTE_Traffic_Erlang] is not null and  (" + Huawei_Volte_list + ") and (" + date_list1_Nokia_G1800 + ")) tble group by Date_Key, Site, Sector")
        Volte_Huawei_4G_Table = conn_apache_Spring.fetchall()
        conn_apache_Spring.execute("select Date_Key, Site, Sector, avg(cast([CSFB_Success_Rate] as float)) as 'CSFB' from ( select Date_Key, substring(Cell_Name,1,8) as 'Site', substring(Cell_Name,1,9) as 'Sector',  [CSFB_Success_Rate] from [Volte_CSFB] where  [CSFB_Success_Rate] is not null and  (" + Huawei_Volte_list + ") and (" + date_list1_Nokia_G1800 + ")) tble group by Date_Key, Site, Sector")
        CSFB_Huawei_4G_Table = conn_apache_Spring.fetchall()

    if Nokia_4G_list!="":
        conn_performanceDB.execute("select Day, Site, Sector, sum([PS_Traffic_Daily (GB)]) as 'PS_Traffic_Daily (GB)', avg([UE DL THR (Mbps)]) as 'UE DL THR (Mbps)',  avg([UE UL THR (Mbps)]) as 'UE UL THR (Mbps)', avg([ERAB Drop Rate]) as 'ERAB Drop Rate', avg([ERAB Setup SR]) as 'ERAB Setup SR', avg([Intra Freq HO SR]) as 'Intra Freq HO SR', avg([RRC Connection SR]) as 'RRC Connection SR' , avg([Cell Availability]) as 'Cell Availability' from (select [Date], substring(convert(varchar,Date,23),1,10) as 'Day', substring([ElementID1],1,8) as 'Site', substring([ElementID1],1,9) as 'Sector', [ElementID1] as 'Cell',   [Total_Payload_GB(Nokia_LTE_CELL)] as 'PS_Traffic_Daily (GB)', [User_Throughput_DL_mbps(Nokia_LTE_CELL)] as 'UE DL THR (Mbps)', [User_Throughput_UL_mbps(Nokia_LTE_CELL)] as 'UE UL THR (Mbps)', [E-RAB_Drop_Ratio_RAN_View(Nokia_LTE_CELL)] as 'ERAB Drop Rate', [E-RAB_Setup_SR_incl_added(Nokia_LTE_CELL)] as 'ERAB Setup SR' , [HO_Success_Ratio_intra_eNB(Nokia_LTE_CELL)] as 'Intra Freq HO SR' , [RRC_Connection_Setup_Success_Ratio(Nokia_LTE_CELL)] as 'RRC Connection SR' , [cell_availability_exclude_manual_blocking(Nokia_LTE_CELL)] as 'Cell Availability' from [dbo].[TBL_LTE_CELL_Daily_N] where  (" + Nokia_4G_list + ") and (" + date_list1 + ")) tble group by Day,  Site, Sector")    
        KPI_Nokia_4G_Table = conn_performanceDB.fetchall()
        conn_apache_Spring.execute("select Date_Key, Site, Sector, sum(cast([VoLTE_Traffic_Erlang] as float)) as 'Volte' from (select Date_Key, substring(Cell_Name,1,8) as 'Site', substring(Cell_Name,1,9) as 'Sector',  [VoLTE_Traffic_Erlang] from [Volte_CSFB] where  [VoLTE_Traffic_Erlang] is not null and  (" + Nokia_Volte_list + ") and (" + date_list1_Nokia_G1800 + ")) tble group by Date_Key, Site, Sector")
        Volte_Nokia_4G_Table = conn_apache_Spring.fetchall()
        conn_apache_Spring.execute("select Date_Key, Site, Sector, avg(cast([CSFB_Success_Rate] as float)) as 'CSFB' from ( select Date_Key, substring(Cell_Name,1,8) as 'Site', substring(Cell_Name,1,9) as 'Sector',  [CSFB_Success_Rate] from [Volte_CSFB] where  [CSFB_Success_Rate] is not null and  (" + Nokia_Volte_list + ") and (" + date_list1_Nokia_G1800 + ")) tble group by Date_Key, Site, Sector")
        CSFB_Nokia_4G_Table = conn_apache_Spring.fetchall()


    

    KPI0_4G_Results=[]
    n = len(CM_4G_Table)+1
    m = 6
    KPI0_4G_Results = [""] * n
    for x in range (n):
        KPI0_4G_Results[x] = [""] * m

    KPI0_4G_Results[0][0] ='ID'
    KPI0_4G_Results[0][1] ='Site'
    KPI0_4G_Results[0][2] ='Statue'
    KPI0_4G_Results[0][3] ='Update_Date'
    KPI0_4G_Results[0][4] ='Rejecteded_List'
    KPI0_4G_Results[0][5] ='NotUpdated_List'


    for i in range(len(CM_4G_Table)):
        Row_4G=str(CM_4G_Table[i])
        Row_4G=Row_4G.split(", ")

        # Finding column number of headers 
        site_header_ind=0 
        for k in range(len(CM_4G_Table[i].cursor_description)):
            hearder_string=str(CM_4G_Table[i].cursor_description[k])
            if hearder_string[2:10]=="location":
                location_header_ind=k
            if hearder_string[2:6]=="site":
                site_header_ind=k
            if hearder_string[2:6]=="tech":
                tech_header_ind=k
            if hearder_string[2:8]=="vendor":
                vendor_header_ind=k
        ID=Row_4G[0]
        ID=int(ID[1:len(ID)])
        if location_header_ind==0:
            Location=""
        else:
            location=Row_4G[location_header_ind]
            location=location[1:len(location)-1]
        if site_header_ind==0:
            Site=""
        else:
            Site=Row_4G[site_header_ind]
            Site=Site[1:len(Site)-1]
        if tech_header_ind==0:
            Band=""
        else:
            Band=Row_4G[tech_header_ind]
        if vendor_header_ind==0:
            Vendor=""
        else:
            Vendor=Row_4G[vendor_header_ind]

        KPI_Table_4G=[]
        KPI_Table_Volte=[]
        if Vendor=="'Nokia'":
            KPI_Table_4G=KPI_Nokia_4G_Table
            KPI_Table_Volte=Volte_Nokia_4G_Table 
            KPI_Table_CSFB=CSFB_Nokia_4G_Table 
        if Vendor=="'Huawei'":
            KPI_Table_4G=KPI_Huawei_4G_Table
            KPI_Table_Volte=Volte_Huawei_4G_Table 
            KPI_Table_CSFB=CSFB_Huawei_4G_Table  
        if Vendor=="'Ericsson'":
            KPI_Table_4G=KPI_Ericsson_4G_Table
            KPI_Table_Volte=Volte_Ericsson_4G_Table 
            KPI_Table_CSFB=CSFB_Ericsson_4G_Table 


        Status="Pass"
        Rejecteded_List=""
        Not_Updated_List=""
        NotUpdated_List=""


        NotUpdated_PS_Traffic_Daily_Count=0
        NotUpdated_UE_DL_THR_Count=0
        NotUpdated_ERAB_Drop_Rate_Count=0
        NotUpdated_ERAB_Setup_SR_Count=0
        NotUpdated_Intra_Freq_HO_SR_Count=0
        NotUpdated_RRC_Connection_SR_Count=0
        NotUpdated_Cell_Availability_Count=0
        NotUpdated_Volte_Traffic_Count=0
        NotUpdated_CSFB_Success_Rate_Count=0



        Cell_Count_4G=0
        Cell_Count_Volte=0
        Cell_Count_CSFB=0

        KPI0_4G_Results[i+1][0] =ID
        KPI0_4G_Results[i+1][1] =Site
        KPI0_4G_Results[i+1][2] =Status
        KPI0_4G_Results[i+1][3] =Currect_date_day
        KPI0_4G_Results[i+1][4] =Rejecteded_List
        KPI0_4G_Results[i+1][5] =NotUpdated_List



        if len(KPI_Table_4G)==0 and len(KPI_Table_Volte)==0 and len(KPI_Table_CSFB)==0:
            KPI0_4G_Results[i+1][0] =ID
            KPI0_4G_Results[i+1][1] =Site
            KPI0_4G_Results[i+1][2] ="No Data"
            KPI0_4G_Results[i+1][3] =Currect_date_day
            KPI0_4G_Results[i+1][4] =""
            KPI0_4G_Results[i+1][5] =""
        if len(KPI_Table_4G)!=0:
            for p in range(len(KPI_Table_4G)):
                Row_4G_KPI=str(KPI_Table_4G[p])
                Row_4G_KPI=Row_4G_KPI.split(", ")
                Candidate_Site=Row_4G_KPI[1]
                Candidate_Site=Candidate_Site[1:len(Candidate_Site)-1]


                if Candidate_Site==Site:
                    Date=Row_4G_KPI[0]
                    Date=Date[2:len(Date)-1]

                    Cell=Row_4G_KPI[2]
                    Cell=Cell[1:len(Cell)-1]

                    if Site=='MA3L0624':
                        gg=0

                    PS_Traffic=Row_4G_KPI[3]
                    UE_DL_THR=Row_4G_KPI[4]
                    ERAB_Drop_Rate=Row_4G_KPI[6]
                    ERAB_Setup_SR=Row_4G_KPI[7]
                    Intra_Freq_HO_SR=Row_4G_KPI[8]
                    RRC_Connection_SR=Row_4G_KPI[9]
                    Cell_Availability=Row_4G_KPI[10]
                    Cell_Availability=Cell_Availability[0:len(Cell_Availability)-1]

                    if (PS_Traffic!="None"):
                        PS_Traffic=float("{:.2f}".format(float(PS_Traffic)))
                    if (UE_DL_THR!="None"):
                        UE_DL_THR=float("{:.2f}".format(float(UE_DL_THR)))
                    if (ERAB_Drop_Rate!="None"):
                        ERAB_Drop_Rate=float("{:.2f}".format(float(ERAB_Drop_Rate)))
                    if (ERAB_Setup_SR!="None"):
                        ERAB_Setup_SR=float("{:.2f}".format(float(ERAB_Setup_SR)))
                    if (Intra_Freq_HO_SR!="None"):
                        Intra_Freq_HO_SR=float("{:.2f}".format(float(Intra_Freq_HO_SR)))
                    if (RRC_Connection_SR!="None"):
                        RRC_Connection_SR=float("{:.2f}".format(float(RRC_Connection_SR)))
                    if (Cell_Availability!="None"):
                        Cell_Availability=float("{:.2f}".format(float(Cell_Availability)))

                    if PS_Traffic!="None":
                        if (PS_Traffic<=PS_Traffic_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" PS_Traffic at "+Date+": "+str(PS_Traffic)+", "
                    else:
                        NotUpdated_PS_Traffic_Daily_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" PS_Traffic at "+Date+": "+str(PS_Traffic)+", "
                    if UE_DL_THR!="None":
                        if (UE_DL_THR<UE_DL_THR_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" UE_DL_THR at "+Date+": "+str(UE_DL_THR)+", "
                    else:
                        NotUpdated_UE_DL_THR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" UE_DL_THR at "+Date+": "+str(UE_DL_THR)+", "
                    if ERAB_Drop_Rate!="None":
                        if (ERAB_Drop_Rate>ERAB_Drop_Rate_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" ERAB_Drop_Rate at "+Date+": "+str(ERAB_Drop_Rate)+", "
                    else:
                        NotUpdated_ERAB_Drop_Rate_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" ERAB_Drop_Rate at "+Date+": "+str(ERAB_Drop_Rate)+", "
                    if ERAB_Setup_SR!="None":
                        if (ERAB_Setup_SR<ERAB_Setup_SR_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" ERAB_Setup_SR at "+Date+": "+str(ERAB_Setup_SR)+", "
                    else:
                        NotUpdated_ERAB_Setup_SR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" ERAB_Setup_SR at "+Date+": "+str(ERAB_Setup_SR)+", "
                    if Intra_Freq_HO_SR!="None":
                        if (Intra_Freq_HO_SR<Intra_Freq_HO_SR_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" Intra_Freq_HO_SR at "+Date+": "+str(Intra_Freq_HO_SR)+", "
                    else:
                        NotUpdated_Intra_Freq_HO_SR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" Intra_Freq_HO_SR at "+Date+": "+str(Intra_Freq_HO_SR)+", "
                    if RRC_Connection_SR!="None":
                        if (RRC_Connection_SR<RRC_Connection_SR_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" RRC_Connection_SR at "+Date+": "+str(RRC_Connection_SR)+", "
                    else:
                        NotUpdated_RRC_Connection_SR_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" RRC_Connection_SR at "+Date+": "+str(RRC_Connection_SR)+", "
                    if Cell_Availability!="None":
                        if (Cell_Availability<Cell_Availability_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" Cell_Availability at "+Date+": "+str(Cell_Availability)+", "
                    else:
                        NotUpdated_Cell_Availability_Count+=1
                        Status='Not Updated'
                        NotUpdated_List=NotUpdated_List+Cell+" Cell_Availability at "+Date+": "+str(Cell_Availability)+", "


                    Cell_Count_4G+=1
                    if (Status=="Not Updated" and (NotUpdated_PS_Traffic_Daily_Count==Cell_Count_4G or NotUpdated_UE_DL_THR_Count==Cell_Count_4G or NotUpdated_ERAB_Drop_Rate_Count==Cell_Count_4G or NotUpdated_ERAB_Setup_SR_Count==Cell_Count_4G or NotUpdated_Intra_Freq_HO_SR_Count==Cell_Count_4G or NotUpdated_RRC_Connection_SR_Count==Cell_Count_4G or NotUpdated_Cell_Availability_Count==Cell_Count_4G)):
                        Status='Not Updated'
                    else:
                       if Status!="Reject":
                           Status='Pass'

                    if Rejecteded_List!="":
                        Status="Reject"
                    KPI0_4G_Results[i+1][0] =ID
                    KPI0_4G_Results[i+1][1] =Site
                    KPI0_4G_Results[i+1][2] =Status
                    KPI0_4G_Results[i+1][3] =Currect_date_day
                    KPI0_4G_Results[i+1][4] =Rejecteded_List
                    KPI0_4G_Results[i+1][5] =NotUpdated_List

            if (Cell_Count_4G==0):
                KPI0_4G_Results[i+1][0] =ID
                KPI0_4G_Results[i+1][1] =Site
                KPI0_4G_Results[i+1][2] ="No Data"
                KPI0_4G_Results[i+1][3] =Currect_date_day
                KPI0_4G_Results[i+1][4] =""
                KPI0_4G_Results[i+1][5] =""

        if len(KPI_Table_Volte)!=0:
            for p in range(len(KPI_Table_Volte)):
                Row_4G_KPI=str(KPI_Table_Volte[p])
                Row_4G_KPI=Row_4G_KPI.split(", ")
                Candidate_Site=Row_4G_KPI[1]
                Candidate_Site=Candidate_Site[1:len(Candidate_Site)-1]

                if Candidate_Site==Site:
                    Date=Row_4G_KPI[0]
                    Date=Date[2:6]+"-"+Date[6:8]+"-"+Date[8:10]

                    Cell=Row_4G_KPI[2]
                    Cell=Cell[1:len(Cell)-1]
                    Volte=Row_4G_KPI[3]


                    if (Volte[0:4]!="None"):
                        Volte=Volte[0:len(Volte)-1]
                        Volte=float(Volte)
               

                    if Volte!="None":
                        if (Volte<=Volte_Traffic_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" Volte at "+Date+": "+str(Volte)+", "

                    if Status!="Reject":
                        Status='Pass'

                    if Rejecteded_List!="":
                        Status="Reject"
                    KPI0_4G_Results[i+1][0] =ID
                    KPI0_4G_Results[i+1][1] =Site
                    KPI0_4G_Results[i+1][2] =Status
                    KPI0_4G_Results[i+1][3] =Currect_date_day
                    KPI0_4G_Results[i+1][4] =Rejecteded_List
                    KPI0_4G_Results[i+1][5] =NotUpdated_List

            if (Cell_Count_4G==0):
                stop=1
                #KPI0_4G_Results[i+1][0] =ID
                #KPI0_4G_Results[i+1][1] =Site
                #KPI0_4G_Results[i+1][2] ="No Data"
                #KPI0_4G_Results[i+1][3] =Currect_date_day
                #KPI0_4G_Results[i+1][4] =""
                #KPI0_4G_Results[i+1][5] =""

        if len(KPI_Table_CSFB)!=0:
            for p in range(len(KPI_Table_CSFB)):
                Row_4G_KPI=str(KPI_Table_CSFB[p])
                Row_4G_KPI=Row_4G_KPI.split(", ")
                Candidate_Site=Row_4G_KPI[1]
                Candidate_Site=Candidate_Site[1:len(Candidate_Site)-1]

                if Candidate_Site==Site:
                    Date=Row_4G_KPI[0]
                    Date=Date[2:6]+"-"+Date[6:8]+"-"+Date[8:10]

                    Cell=Row_4G_KPI[2]
                    Cell=Cell[1:len(Cell)-1]
                    CSFB=Row_4G_KPI[3]


                    if (CSFB[0:4]!="None"):
                        CSFB=CSFB[0:len(CSFB)-1]
                        CSFB=float(CSFB)
               

                    if CSFB!="None":
                        if (CSFB<CSFB_Success_Rate_4G):
                            Status='Reject'
                            Rejecteded_List=Rejecteded_List+Cell+" CSFB at "+Date+": "+str(CSFB)+", "

                    if Status!="Reject":
                        Status='Pass'

                    if Rejecteded_List!="":
                        Status="Reject"
                    KPI0_4G_Results[i+1][0] =ID
                    KPI0_4G_Results[i+1][1] =Site
                    KPI0_4G_Results[i+1][2] =Status
                    KPI0_4G_Results[i+1][3] =Currect_date_day
                    KPI0_4G_Results[i+1][4] =Rejecteded_List
                    KPI0_4G_Results[i+1][5] =NotUpdated_List

            if (Cell_Count_4G==0):
                stop=1
                #KPI0_4G_Results[i+1][0] =ID
                #KPI0_4G_Results[i+1][1] =Site
                #KPI0_4G_Results[i+1][2] ="No Data"
                #KPI0_4G_Results[i+1][3] =Currect_date_day
                #KPI0_4G_Results[i+1][4] =""
                #KPI0_4G_Results[i+1][5] =""


    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()



    for n in range(len(KPI0_4G_Results)-1):
        Update_String_Quary="UPDATE CM SET kpiZDate = '"+KPI0_4G_Results[n+1][3]+"' , kpiZStatus ='"+KPI0_4G_Results[n+1][2]+"' WHERE id="+ str(KPI0_4G_Results[n+1][0])+ " and Site='"+KPI0_4G_Results[n+1][1]+"'"
        conn_apache_Desk.execute(Update_String_Quary)
        conn_apache_Desk.commit()


    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()


    for n in range(len(KPI0_4G_Results)-1):
        Update_String_Quary="UPDATE CM SET CMStatus = 4  , pendingTeam='SSV' WHERE  kpiZStatus = 'Pass' and id="+ str(KPI0_4G_Results[n+1][0])+ " and Site='"+KPI0_4G_Results[n+1][1]+"'"
        conn_apache_Desk.execute(Update_String_Quary)
        conn_apache_Desk.commit()


    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()



    for n in range(len(KPI0_4G_Results)-1):
        CMID=KPI0_4G_Results[n+1][0]
        userID=0
        mail="spring@nak-mci.ir"
        if KPI0_4G_Results[n+1][2]=="Pass":
            activityType="pass"
        else:
            activityType=""
        activityDate=datetime.datetime.now()
        site=KPI0_4G_Results[n+1][1]
        location=site[0:3]+Site[4:8]
        status="KPIZeo "+KPI0_4G_Results[n+1][2]
        conn_apache_Desk.execute('''INSERT INTO CM_Activity (CMID,userID,mail,activityType,activityDate,siteId,locationId,action) VALUES (?,?,?,?,?,?,?,?) ''',
        CMID,
        userID,
        mail,
        activityType,
        activityDate,
        site,
        location,
        status
        )
        conn_apache_Desk.commit()


    # Connection to apache_nifi
    conn_apache_Desk = pyodbc.connect('Driver={SQL Server};'
                          'Server=apache_nifi;'
                          'Database=Desk;'
                          'Trusted_Connection=yes;')
    conn_apache_Desk = conn_apache_Desk.cursor()



    for n in range(len(KPI0_4G_Results)-1):
        conn_apache_Desk.execute('''INSERT INTO KPIZeroStatus (CMID,Site,KPIZDate,KPIZStatus,Rejected_List,Non_Updated_List) VALUES (?,?,?,?,?,?) ''',
        KPI0_4G_Results[n+1][0],
        KPI0_4G_Results[n+1][1],
        KPI0_4G_Results[n+1][3],
        KPI0_4G_Results[n+1][2],
        KPI0_4G_Results[n+1][4],
        KPI0_4G_Results[n+1][5]
        )
        conn_apache_Desk.commit()



conn_apache_Desk.close()
conn_apache_Spring.close()
conn_performanceDB.close()





with open('Logfile.txt', 'a+') as f1:
    f1.write('\n'+str(datetime.datetime.now()))
