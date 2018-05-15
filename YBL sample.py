# -*- coding: utf-8 -*-
"""
Created on Sun Mar  4 10:26:27 2018

@author: chenfa
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Dec 30 17:42:09 2017

@author: chenfa
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 12:03:42 2017

@author: chenfa
"""

import requests
import os
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import pandas as pd
#pd.set_option('display.float_format', lambda x: '%.2f' % x)
from bs4 import BeautifulSoup
from selenium import webdriver
import copy
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import pyodbc
import numpy as np
#Prius
import datetime
#email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import tkinter


#os.path.dirname(os.path.abspath('YBLauto3.2.py'))
dir_path = 

url = [["bg", "https://ipc-bm.amazon.com/inbound/inbound/view_transpose?org=US&report_id="],
       ["bgca", "https://ipc-bm.amazon.com/inbound/inbound/view_transpose?org=CA&report_id="],
       ["cu", "https://capacity.amazon.com/sccp/101/storage_requirement_metric"],
       ["cuca", "https://capacity.amazon.com/sccp/115/storage_requirement_metric"]]



CA = ['YVR2','YVR3','YYZ1','YYZ2','YYZ3','YYZ4','YYZ6']
#USFC = IXD+SortTraditional+SortAR+SortQD+SortSupplemental+Specialty+NonSortTraditional+NonSortAR+ThirdPL+SpecialHandling

#FCs = USFC 
#Future use 
USFCd = {}

USFC = USFCd['IXD']+USFCd['SortTraditional']+USFCd['SortAR']+USFCd['SortQD']+USFCd['SortSupplemental']+USFCd['Specialty']+USFCd['NonSortTraditional']+USFCd['NonSortAR']+USFCd['ThirdPL']+USFCd['SpecialHandling']

NetWork = []
for i in USFC:
    if i in USFCd['IXD']:
        NetWork.append('IXD')
    elif i in USFCd['SortTraditional']:
        NetWork.append('SortTraditional')
    elif i in USFCd['SortAR']:
        NetWork.append('SortAR')
    elif i in USFCd['SortQD']:
        NetWork.append('SortQD')
    elif i in USFCd['SortSupplemental']:
        NetWork.append('SortSupplemental')
    elif i in USFCd['Specialty']:
        NetWork.append('Specialty')
    elif i in USFCd['NonSortTraditional']:
        NetWork.append('NonSortTraditional')
    elif i in USFCd['NonSortAR']:
        NetWork.append('NonSortAR')
    elif i in USFCd['ThirdPL']:
        NetWork.append('ThirdPL')
    elif i in USFCd['SpecialHandling']:
        NetWork.append('SpecialHandling')
        
#index =pd.MultiIndex.from_arrays([NetWork,USFC], names = ['NetWork','FCs'])
USFCdf = pd.DataFrame(USFC, columns=['FC'], index = pd.MultiIndex.from_arrays([NetWork,USFC], names = ['NetWork','FCs']))
#indexCA = pd.MultiIndex.from_arrays([['CA' for i in CA],CA], names = ['NetWork','FCs'])
CAdf = pd.DataFrame(CA, columns=['FC'], index = pd.MultiIndex.from_arrays([['CA' for i in CA],CA], names = ['NetWork','FCs']) )
FCtypes = ['AmazonNetwork','IXD','SortTraditional','SortAR','SortQD','SortSupplemental','Specialty','NonSortTraditional','NonSortAR','ThirdPL','SpecialHandling']
FCtypeDF= pd.DataFrame(FCtypes, columns=['FC'],index = pd.MultiIndex.from_arrays([['FC Type' for i in FCtypes],FCtypes], names = ['NetWork','FCs']) ) 
#FCtypeDF= pd.DataFrame(FCtypes, columns=['FC']) 
#FCtypeDF = FCtypeDF.set_index([FCtypes])

cert = r"C:\Users\chenfa\Documents\YardLog\cacerts.pem"
os.environ["REQUESTS_CA_BUNDLE"] = cert
kerberos = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
#prius = open('prius_data2.txt','w')
def priusData(USFC):
    now = datetime.datetime.now()
    table = []
    for FC in USFC:
        try:
            url = 'https://inbound.amazon.com/appt_outlook?utf8=%E2%9C%93&fc='+FC+'&org=US&commit=Update'
            session = requests.Session()
            request = session.get(url, auth=kerberos).content.decode("latin-1")
            soup = BeautifulSoup(request,"lxml")
            s = soup.find('tfoot')
    
            #headers = [header.text for header in s[2].find_all('th')]
            try:
                rows = ([val.text for val in s.find_all('td')]) #.encode('utf8')
                if rows[0] == 'Total':
                    rows[0] = FC
                    print(datetime.datetime.now() - now,rows)
                    table.append(rows)
            except AttributeError:
                print('WebPage is not available')   
            except IndexError:
                print('No data is found')
                #srows[i].append([row.find_all('td')[-1].contents[2]])
        except ConnectionError:
            print('WebPage is not available')
    prius = pd.DataFrame(table)        
    prius.to_csv(dir_path+'\\pruis.csv', index = False, header = None )
    print('Prius Data Gotten')
    return prius
    

#priusCA
def priusCA():
    url = 'https://inbound.amazon.com/appt_outlook?commit=Update&fc=all&org=CA&utf8=?'
    session = requests.Session()
    request = session.get(url, auth=kerberos).content.decode("latin-1")
    soup = BeautifulSoup(request,"lxml")
    table = soup.find_all("table")[1]#,{'class': 'data-grid table table-bordered table-hover appointment-data tablesorter tablesorter-default'})
    df = pd.read_html(str(table), flavor="bs4")[0]
    df.to_csv(dir_path+'\\pruisCA.csv', index = False, header = None )
    print('PriusCA Gotten')
    return df

# prius(FCs)


def wtpData():
    r = requests.get('http://neo-r4-8xl-349f1d13.us-east-1.amazon.com:9998/MNTtp/metric/wtp_master.csv', verify=cert, auth=kerberos)
    data = r.text

    import sys


    if sys.version_info[0] < 3:
        from StringIO import StringIO
    else:
        from io import StringIO

    data = StringIO(data)

    df = pd.read_csv(data, sep=",")
    df.to_csv(dir_path+'\\wtp_master.csv', index=False)
    print('WTP Downladed')
    return df
    

# CU and BG

def read(html):
    html_soup = BeautifulSoup(html, 'lxml')
    tables = []
    table_div = html_soup.find('div', {'id': 'container'})
    # table = table_div.findAll("table")[]
    tables_html = table_div.find_all("table", {'class': 'GG3VN1MDCD table-bordered'})
    # Parse each table
    for n in range(0, len(tables_html)):

        n_cols = 0
        n_rows = 0

        for row in tables_html[n].find_all("tr"):
            col_tags = row.find_all(["td", "th"])
            if len(col_tags) > 0:
                n_rows += 1
                if len(col_tags) > n_cols:
                    n_cols = len(col_tags)

        # Create dataframe
        df = pd.DataFrame(index=range(0, n_rows), columns=range(0, n_cols))

        skip_index = [0 for i in range(0, n_cols)]
        # Create list to store rowspan values

        # Start by iterating over each row in this table...
        row_counter = 0
        for row in tables_html[n].find_all("tr"):

            # Skip row if it's blank
            if len(row.find_all(["td", "th"])) == 0:
                next()

            else:

                # Get all cells containing data in this row
                columns = row.find_all(["td", "th"])
                col_dim = []
                row_dim = []
                col_dim_counter = -1
                row_dim_counter = -1
                col_counter = -1
                this_skip_index = copy.deepcopy(skip_index)

                for col in columns:

                    # Determine cell dimensions
                    colspan = col.get("colspan")
                    if colspan is None:
                        col_dim.append(1)
                    else:
                        col_dim.append(int(colspan))
                    col_dim_counter += 1

                    rowspan = col.get("rowspan")
                    if rowspan is None:
                        row_dim.append(1)
                    else:
                        row_dim.append(int(rowspan))
                    row_dim_counter += 1

                    # Adjust column counter
                    if col_counter == -1:
                        col_counter = 0
                    else:
                        col_counter = col_counter + col_dim[col_dim_counter - 1]

                    while skip_index[col_counter] > 0:
                        col_counter += 1

                    # Get cell contents
                    cell_data = col.get_text()

                    # Insert data into cell
                    df.iat[row_counter, col_counter] = cell_data

                    # Record column skipping index
                    if row_dim[row_dim_counter] > 1:
                        this_skip_index[col_counter] = row_dim[row_dim_counter]

            # Adjust row counter
            row_counter += 1

            # Adjust column skipping index
            skip_index = [i - 1 if i > 0 else i for i in this_skip_index]

        # Append dataframe to list of tables
        tables.append(df)

    return (tables)



    
def read_bg(url):
    driver.get(url[1])
    WebDriverWait(driver, 50).until(EC.visibility_of_all_elements_located((By.ID, "inbound/inbound-view_transpose")))
    html = (driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML"))
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table', {'class': 'table table-condensed table-hover table-report'})

    for body in table('tbody'):
        body.unwrap()

    df = pd.read_html(str(table), flavor="bs4")
    df = pd.DataFrame(df[0])
    df.to_csv(dir_path + '\\'+ url[0] + '.csv', index=False)
    print(url[0]+'gotten')
    return df
    

def read_cu(url):
    driver.get(url[1])
    time.sleep(5)
    wait = WebDriverWait(driver, 50)
    wait.until(EC.element_to_be_clickable((By.ID, "show_real_time_inventory_checkbox"))).click()
    driver.find_element_by_xpath("//input[@value ='Table']").click()
    WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.ID, "CPMetricTableNONSORTABLE")))
    html = (driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML"))
    x = read(html)
    df =  pd.concat(x)
    df.to_csv(dir_path +'\\'+ url[0] + '.csv', index=False)
    print(url[0]+'gotten')
    return df
    
#    for i in webbook:
#        if i[0].startswith('bg'):
#            read_bg(i)
#        elif i[0].startswith('cu'):
#            read_cu(i)
    



#InTriData
def inTridata():
    con=pyodbc.connect(r'DSN=InbTriData;UID=inbpri_ro;DESCRIPTION=Inbound Pri;SERVER={inbpri-export-db-ro.cijru1ctbrlc.us-east-1.rds.amazonaws.com};DATABASE=inbpri_us_export;PORT=8192;')
#    cursor = con.cursor()
    
    query = """"""
    
    #cursor.execute(query)
    #
    #data =cursor.fetchall()
    
    #InbPri Pivot Table (Step 2)
    InbPri = pd.read_sql_query(query, con)
    
    InbPriBLOnSite = InbPri[InbPri.loc[:]['BACKLOG_TYPE']=='ONSITE_BACKLOG']
    
    InbPriBLOnSite.to_csv(dir_path+'\\InbPri.csv', index = False)
    print('Inbri Gotten')
    return pd.pivot_table(InbPriBLOnSite, index =['WAREHOUSE_ID'], values = ['APPT_ESTIMATED_QUANTITY','INBOUND_SHIP_APPOINTMENT_ID'],aggfunc={'APPT_ESTIMATED_QUANTITY':np.sum,'INBOUND_SHIP_APPOINTMENT_ID':np.count_nonzero} )



#Generate report
def report():
#WTP Pivot Tables(Step 7)
#############################
#lambda way to get HourDiff
    now = datetime.datetime.now()
    def hour_diff(function, *args):
        print('Hourdiff')
        current = now
        estarr = datetime.datetime.strptime(function(*args), '%H:%M')
        if current.hour<estarr.hour:
            return pd.Series([round((current-estarr).seconds/3600-24,2)])
        else:
            return pd.Series([round((current-estarr).seconds/3600,2)])
        
    #local time nowtime.strftime("%H:%M",time.localtime())
    def est_arr(col):
        if pd.isnull(col):
            return '00:00'
        else:
            return col[-5:].lstrip()
        
    wtp['HourDiff'] = wtp.apply(lambda x: hour_diff(est_arr,x['Estimated_Arrival']), axis = 1)
    
    #day diff lamba way
    #days of difference in terms of 24 hours 
    
    
    #Old way to get hour diff
    #def hour_diff(function, *args):
    #    current = datetime.datetime.now()
    #    estarr = datetime.datetime.strptime(function(*args), '%H:%M')
    #    if current.hour<estarr.hour:
    #        return round((current-estarr).seconds/3600-24,2)
    #    else:
    #        return round((current- estarr).seconds/3600,2)
    ##local time now          
    #time.strftime("%H:%M",time.localtime())
    #def est_arr(rows, col):
    #    if pd.isnull(rows[col]):
    #        return '00:00'
    #    else:
    #        return rows[col][-5:].lstrip()
    #
    #hourdiff = []
    #
    #for index in range(len(wtp)):
    #    hourdiff.append(hour_diff(est_arr,wtp.iloc[index][:],'Estimated_Arrival'))
    #print(hourdiff)
    #read_web(url)
    #from xlwings import Book
    
    ###
    #
    def day_diff(col):
        print('DayDiff')
        today =datetime.datetime.today().date()
    #    today =datetime.datetime.today().strftime('%Y-%m-%d')
    #    today = datetime.datetime.strptime(today,'%Y-%m-%d')
    #    import re
    #    r = re.compile('.{2}-.{3}-.{2}')
    #    if r.match(col) is not None:
        try:
            trans_Flagday = datetime.datetime.strptime(col,'%d-%b-%y').date()
    #        print(col)
    #        print(trans_Flagday-today)
            return abs((trans_Flagday-today).days)       
        except ValueError:
            print('Not a Date')
            return np.nan
        except TypeError:
            print('Not a Date')
            return np.nan   
        
    wtp['day_diff'] = wtp.apply(lambda x: day_diff(x['trans_flag']), axis = 1)
        
    def time_arrival(function,arg1, col2):
        print('TimeArrival')
        if pd.isnull(function(arg1)):
            return pd.Series([-5])
        else:
            return pd.Series([function(arg1)*24 - float(col2)])
        
    wtp['TimeArrival'] = wtp.apply(lambda x: time_arrival(day_diff,x['trans_flag'],x['HourDiff']), axis = 1)
        
    #super older way
    #int(wtp.iloc[1]['Order_date'])
    #
    #timearri = []
    #
    #for index in range(len(wtp)):
    #    timearri.append(time_arrival(wtp.iloc[index][:],'Order_date'))
    
    def flag(col1,col2):
        print('Flag')
        x = ' ' 
        if pd.isnull(col1):
            pass
        elif col1 == 'IN_YARD' or col1 == 'STOW' or col1 =='PS-IY' or col1 =='UNT-IY':
            x = 'IN_YARD'
        elif col1 == 'PS-IT':
            x = col1
        elif col1.startswith('DA'):
            x = 'DA'
        else:
            if col2 <= -5:
                x = 'IN_YARD'
            elif col2<=6 and col2>-5:
                x = '6'
            elif col2 >6 and col2 <=12:
                x = '12'
            elif col2 >12 and col2 <=24:
                x = '24'
            elif col2 >24 and col2 <=48:
                x = '48'
            elif col2 >48 and col2 <=72:
                x = '72'
            elif col2 >72 and col2 <=96:
                x = '96'
            elif col2 >96 and col2 <=120:
                x = '120'
            else:
                x ='120plus'
        return pd.Series([x])
    
    wtp['FLAG2'] = wtp.apply(lambda x: flag(x['trans_flag'],x['TimeArrival']), axis = 1)
    
    #Count Units
    wtpPriPivot1 = pd.pivot_table(wtp, index =['destination'], columns = ['FLAG2'], values = ['units'],aggfunc={'units':np.sum}, fill_value = 0, margins = True, margins_name = 'GrandTotal', dropna = True)
    wtpPriPivot1 = wtpPriPivot1.astype(int)
    
    #Sum VrID
    wtpPriPivot2 = pd.pivot_table(wtp, index =['destination'], columns = ['FLAG2'], values = ['vrid'],aggfunc={'vrid':np.count_nonzero}, fill_value = 0, margins = True, margins_name = 'GrandTotal', dropna = True)
    wtpPriPivot2 = wtpPriPivot2.fillna(0).astype(int)
    
    
    #######Matrix 
    #USFClist = [IXD,SortTraditional,SortAR,SortQD,SortSupplemental,Specialty,NonSortTraditonal,NonSortAR,ThirdPL,SpecialHandling]
    #
    #dict({i:eval(i) for i in 'IXD'}) 
    #for i in USFC:
    #    for group in USFClist:
    #        for i in group:
    #            NetWork.append(gorup.key.value)  for future refinement-Not hard coding
                
    
    
    #row.name return index of the row
    def bgDaysBackLog(df1,df2, colName, colIndex):
        print('bgDayBacklog')
        indexOfDEVFB = df2.index[df2.iloc[:,0] == colName].values[0]
        indexofFCs = df2[df2[0] == df1['FC']].index.tolist()
        if indexofFCs:
            distance = []
            for i in indexofFCs:
                distance.append(i-indexOfDEVFB)
            indexofFC = indexofFCs[distance.index(min([i for i in distance if i >0]))]
    #        print(indexOfDEVFB,indexofFCs,distance, indexofFC)
            return pd.Series([float(df2.iloc[indexofFC,colIndex])])
        else:
            return 0
    #
    #indexOfDEVFB = bg.index[bg.iloc[:,0] == 'Days of End Vendor Freight Backlog'].values[0]
    #indexofFCs = bg[bg[0] == 'ONT8'].index.tolist()
    #distance = []
    #for i in indexofFCs:
    #    distance.append(i-indexOfDEVFB)
    #indexofFC = indexofFCs[distance.index(min([i for i in distance if i >0]))]
    #days = bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:]
    #bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:].index[bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:]==now.strftime('%d-%b')].values
    USFCdf['DaysVendorBacklog[Today]'] = USFCdf.apply(lambda x: bgDaysBackLog(x, bg,'Days of End Vendor Freight Backlog',bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:].index[bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:]==now.strftime('%d-%b')].values), axis=1)
    CAdf['DaysVendorBacklog[Today]'] = CAdf.apply(lambda x: bgDaysBackLog(x, bgca,'Days of End Vendor Freight Backlog',bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:].index[bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:]==now.strftime('%d-%b')].values), axis=1)
    
    #Mean Days Pushed 
    def MDP(df1,df2):
        print('MDP')
        indexofFC = df2[df2.iloc[:,0] == df1['FC']].index.tolist()
    
        if indexofFC:
            return pd.Series([float(df2.iloc[indexofFC[0],8])])
        else:
            return pd.Series([' '])
    
    
    
    
    USFCdf['MeanDaysPushed'] = USFCdf.apply(lambda x: MDP(x, prius), axis=1)
    CAdf['MeanDaysPushed'] = CAdf.apply(lambda x: MDP(x, priusca), axis=1)
    
    #VendorTrucksinYard and VendorUnitsinYard
    def PivotMatrix(df1,df2, colName):
        try:
            return pd.Series([df2.loc[df1['FC']][colName]])
        except KeyError:
            print(df1['FC']+' data is not available')
            return pd.Series([' ']) 
        
    USFCdf['VendorTrucksinYard'] = USFCdf.apply(lambda x: PivotMatrix(x, InbPriPivot,'INBOUND_SHIP_APPOINTMENT_ID'), axis=1)
    CAdf['VendorTrucksinYard'] = CAdf.apply(lambda x: PivotMatrix(x, InbPriPivot,'INBOUND_SHIP_APPOINTMENT_ID'), axis=1)
    
    USFCdf['VendorUnitsinYard'] = USFCdf.apply(lambda x: PivotMatrix(x, InbPriPivot,'APPT_ESTIMATED_QUANTITY'), axis=1)
    CAdf['VendorUnitsinYard'] = CAdf.apply(lambda x: PivotMatrix(x, InbPriPivot,'APPT_ESTIMATED_QUANTITY'), axis=1)
    
    #DaysTransBacklog[Today]
    USFCdf['DaysTransBacklog[Today]'] = USFCdf.apply(lambda x: bgDaysBackLog(x, bg,"Days of Transfer In Backlog",bg.iloc[bg.index[bg[0]=="Days of Transfer In Backlog"].values,:].iloc[0,:].index[bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:]==now.strftime('%d-%b')].values), axis=1)
    CAdf['DaysTransBacklog[Today]'] = CAdf.apply(lambda x: bgDaysBackLog(x, bgca,"Days of Transfer In Backlog",bg.iloc[bg.index[bg[0]=="Days of Transfer In Backlog"].values,:].iloc[0,:].index[bg.iloc[bg.index[bg[0]=='Days of End Vendor Freight Backlog'].values,:].iloc[0,:]==now.strftime('%d-%b')].values), axis=1)
    
    #TransInTrucksinYard
    USFCdf['TransInTrucksinYard'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot2['vrid'],'IN_YARD'), axis=1)
    CAdf['TransInTrucksinYard'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot2['vrid'],'IN_YARD'), axis=1)
    #TransInUnitesInYard&NYR
    USFCdf['TransInUnitesInYard&NYR'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'IN_YARD'), axis=1)
    CAdf['TransInUnitesInYard&NYR'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'IN_YARD'), axis=1)
    
    #TransInTrucksInTransit 
    def PivotMatrixcalculation(df1,df2, colName1, colName2):
        print('PviotCalculation')
        pivotValue = 0
        try:
            pivotValue = df2.loc[df1['FC']][colName1]
        except KeyError:
            print(df1['FC']+' data is not available')
        dfValue = df1[colName2]
        if dfValue != ' ':
            return pd.Series([pivotValue - dfValue])
        else:
            return pd.Series([' ']) 
    
    USFCdf['TransInTrucksInTransit'] = USFCdf.apply(lambda x: PivotMatrixcalculation(x, wtpPriPivot2['vrid'],'GrandTotal', 'TransInTrucksinYard'), axis=1)          
    CAdf['TransInTrucksInTransit'] = CAdf.apply(lambda x: PivotMatrixcalculation(x, wtpPriPivot2['vrid'],'GrandTotal', 'TransInTrucksinYard'), axis=1)       
    
    
    #TransInUnitesinTransit
    USFCdf['TransInUnitesInTransit'] = USFCdf.apply(lambda x: PivotMatrixcalculation(x, wtpPriPivot1['units'],'GrandTotal', 'TransInUnitesInYard&NYR'), axis=1)          
    CAdf['TransInUnitesInTransit'] = CAdf.apply(lambda x: PivotMatrixcalculation(x, wtpPriPivot1['units'],'GrandTotal', 'TransInUnitesInYard&NYR'), axis=1)       
    
    #PastDue
    USFCdf['PastDue'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'DA'), axis=1)
    CAdf['PastDue'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'DA'), axis=1)
    
    #PastDueTrucks 
    USFCdf['PastDueTrucks '] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot2['vrid'],'DA'), axis=1)
    CAdf['PastDueTrucks '] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot2['vrid'],'DA'), axis=1)
    
    #0-6Hours
    USFCdf['0-6Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'6'), axis=1)
    CAdf['0-6Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'6'), axis=1)
    
    #6-12Hours
    USFCdf['6-12Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'12'), axis=1)
    CAdf['6-12Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'12'), axis=1)
    
    #12-24Hours
    USFCdf['12-24Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'24'), axis=1)
    CAdf['12-24Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'24'), axis=1)
    
    #24-48Hours
    USFCdf['24-48Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'48'), axis=1)
    CAdf['24-48Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'48'), axis=1)
    
    #48-72Hours
    USFCdf['48-72Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'72'), axis=1)
    CAdf['48-72Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'72'), axis=1)
    
    #72-96Hours
    USFCdf['72-96Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'96'), axis=1)
    CAdf['72-96Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'96'), axis=1)
    
    #96-120Hours
    USFCdf['96-120Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'120'), axis=1)
    CAdf['96-120Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'120'), axis=1)
    #120+
    #USFCdf['120+Hours'] = USFCdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'120plus'), axis=1)
    #CAdf['120+Hours'] = CAdf.apply(lambda x: PivotMatrix(x, wtpPriPivot1['units'],'120plus'), axis=1)
    
    
    #Cu
    def cuDaysBackLog(df1,df2,lookupValue,colIndex):
        print('CuDayBacklog')
        indexOfFC = df2.index[df2.iloc[:,0] == df1['FC']].values
        indexofLookUps = df2[df2[0] == lookupValue].index.tolist()
        if indexofLookUps and indexOfFC.size>0:
            distance = []
            for i in indexofLookUps:
                distance.append(i-indexOfFC)
            indexoflookUp = indexofLookUps[distance.index(min([i for i in distance if i >0]))]
    #        print(indexOfDEVFB,indexofFCs,distance, indexofFC)
            return pd.Series([df2.iloc[indexoflookUp,colIndex]])
        else:
            return pd.Series(['0%'])
       
    #Total Fullness
    USFCdf['Total Fullness'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'FC Total Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    CAdf['Total Fullness'] = CAdf.apply(lambda x: cuDaysBackLog(x, cuca,'FC Total Usage',cuca.iloc[1,:][cuca.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    
    #Library
    USFCdf['Library'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Library Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    CAdf['Library'] = CAdf.apply(lambda x: cuDaysBackLog(x, cuca,'Library Usage',cuca.iloc[1,:][cuca.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    
    #LibraryDeep
    USFCdf['LibraryDeep'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Library Deep Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    CAdf['LibraryDeep'] = CAdf.apply(lambda x: cuDaysBackLog(x, cuca,'Library Deep Usage',cuca.iloc[1,:][cuca.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    
    #PrimePallet
    USFCdf['PrimePallet'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Prime Pallet Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    CAdf['PrimePallet'] = CAdf.apply(lambda x: cuDaysBackLog(x, cuca,'Prime Pallet Usage',cuca.iloc[1,:][cuca.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    
    #ReservePallet      
    USFCdf['ReservePallet'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Reserve Pallet Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    CAdf['ReservePallet'] = CAdf.apply(lambda x: cuDaysBackLog(x, cuca,'Reserve Pallet Usage',cuca.iloc[1,:][cuca.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    
    #USFCdf.loc[:,'TransInTrucksinYard':'96-120Hours'].astype(int,errors = 'ignore')
    #USFCdf['TransInTrucksinYard'] = pd.Series(["{:,.0f}".formatval for val in USFCdf['TransInTrucksinYard'] if type(val) == float ], index = df.index)
    #  
    USFCdf.loc['IXD','DaysTransBacklog[Today]':'ReservePallet']=' '
    
    
     
    #Sum up  withou lambda   
    #def summary(df,FClist, col2):
    ##    print(col2)
    #    Sum = {}
    #    for i in df.index.tolist():
    #        total = 0
    ##        print(i)
    #        if i =='AmazonNetwork':
    #            pass 
    #        else:
    #            for FC in FClist[i]:
    ##                print(FC)
    #                try:
    #                    if USFCdf.loc[i][:].loc[FC][col2] != ' ':
    ##                        print(USFCdf.loc[i][:].loc[FC][col2])
    #                        total +=USFCdf.loc[i][:].loc[FC][col2] 
    #                    else:
    #                        total +=0
    #                except KeyError:
    #                    print('No Data')
    ##        print(total)
    #        Sum.update({i:total})
    #    return Sum
    
    def Testsummary(df,FClist, col2):
        print('summary')
    #    print(col2)
    #    print(df['FC'])
    #    for i in df.index.tolist():
        total = 0
    ##        print(i)
        if df['FC'] =='AmazonNetwork':
            pass 
        else:
            for FC in FClist[df['FC']]:
    #                print(FC)
                try:
                    if USFCdf.loc[df['FC']][:].loc[FC][col2] != ' ':
    #                        print(USFCdf.loc[i][:].loc[FC][col2])
                        total +=USFCdf.loc[df['FC']][:].loc[FC][col2] 
                    else:
                        total +=0
                except KeyError:
                    print('No Data')
    #        print(total)
        return pd.Series([total])
     
    #summary(FCtypeDF, USFCd,'TransInTrucksInTransit')
    for i in USFCdf.columns[1:19]:
        FCtypeDF[i] = FCtypeDF.apply(lambda x: Testsummary(x, USFCd, i),axis= 1 )
    
    #with out lambda
    #for i in USFCdf.columns[1:19]:
    #    FCtypeDF = FCtypeDF.merge(pd.DataFrame(pd.Series(summary(FCtypeDF,USFCd, i)),columns =[i]),how = 'inner', left_index = True, right_index = True, sort = False)    
    
    #CuSummary without Lambda
    def cuBLSum(df2,lookupValue,colIndex):
        value =[]
        list1 = [' ', ' ', 'Total SORTABLE' ,' ', ' ', ' ', 'Total SPECIALTY', 'Total NONSORTABLE',' ', 'Total 3PL', ' ']
        for j in list1:
            if j == ' ':
                value.append(' ')
            else:
                indexOfFC = df2.index[df2.iloc[:,0] == j].values
                indexofLookUps = df2[df2[0] == lookupValue].index.tolist()
                if indexofLookUps.size and indexOfFC.size>0:
                    distance = []
                    for i in indexofLookUps:
                        distance.append(i-indexOfFC)
                    indexoflookUp = indexofLookUps[distance.index(min([i for i in distance if i >0]))]
            #        print(indexOfDEVFB,indexofFCs,distance, indexofFC)
                    value.append(df2.iloc[indexoflookUp,colIndex])
                else:
                    value.append('0%')
        return pd.Series(value) 
    #Total Fullness    
    FCtypeDF['Total Fullness'] = cuBLSum(cu, 'FC Total Usage', cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]).values
    #Library
    FCtypeDF['Library'] = cuBLSum(cu,'Library Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]).values
    
    #LibraryDeep
    FCtypeDF['LibraryDeep'] = cuBLSum(cu,'Library Deep Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]).values
    
    #PrimePallet
    FCtypeDF['PrimePallet'] = cuBLSum(cu,'Prime Pallet Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]).values
    
    #ReservePallet      
    FCtypeDF['ReservePallet'] = cuBLSum( cu,'Reserve Pallet Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]).values 
    ##CU with lambda
    #def cuBLSum(df1, df2,lookupValue,colIndex):
    #    value = 0
    #    print(df['FC'])
    #    if df['FC'] =='AmazonNetwork':
    #        pass
    #    else:
    #        list1 = {'IXD': ' ', 'SortTraditional': 'Total SORTABLE' ,'SortAR': ' ','SortQD': ' ', 'SortSupplemental':' ','Specialty':'Total SPECIALTY', 'NonSortTraditional ':'Total NONSORTABLE','NonSortAR':' ', 'ThirdPL':'Total 3PL','SpecialHandling': ' '}
    #        if list1[df['FC']] == ' ':
    #            value = ' '
    #        else:
    #            indexOfFC = df2.index[df2.iloc[:,0] == list1[df['FC']]].values
    #            indexofLookUps = df2[df2[0] == lookupValue].index.tolist()
    #            if indexofLookUps and indexOfFC:
    #                distance = []
    #                for i in indexofLookUps:
    #                    distance.append(i-indexOfFC)
    #                indexoflookUp = indexofLookUps[distance.index(min([i for i in distance if i >0]))]
    #        #        print(indexOfDEVFB,indexofFCs,distance, indexofFC)
    #                value = df2.iloc[indexoflookUp,colIndex]
    #            else:
    #                value= '0%'
    #        return pd.Series([value]) 
    #
    ##Total Fullness
    #FCtypeDF['Total Fullness'] = FCtypeDF.apply(lambda x: cuBLSum(x, cu,'FC Total Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)    
    ##Library
    #FCtypeDF['Library'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Library Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    
    ##LibraryDeep
    #FCtypeDF['LibraryDeep'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Library Deep Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    #
    ##PrimePallet
    #FCtypeDF['PrimePallet'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Prime Pallet Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    #
    ##ReservePallet      
    #FCtypeDF['ReservePallet'] = USFCdf.apply(lambda x: cuDaysBackLog(x, cu,'Reserve Pallet Usage',cu.iloc[1,:][cu.iloc[1,:]=='Realtime Actuals'].index.values[0]), axis=1)
    # 
    
    
    for i in FCtypeDF.columns[1:19]:
        FCtypeDF.loc['FC Type'].loc['AmazonNetwork',i] = FCtypeDF.loc['FC Type'].loc['IXD':'SpecialHandling',i].sum()      
    
    FCtypeDF['DaysVendorBacklog[Today]'] = ' '
    FCtypeDF['MeanDaysPushed'] = ' '
    FCtypeDF['DaysTransBacklog[Today]'] = ' '
    
    Matric = pd.concat([FCtypeDF,USFCdf,CAdf])
    Matric = Matric.iloc[:,1:]
    
    def thousandFormatting(number):
        if type(number) == int:
            return '{:,}'.format(number)
        else:
            return number
            
    for col in Matric.columns:
        Matric[col] = Matric[col].apply(thousandFormatting)
            
            
                
    
        
    
    
    htmlTable = Matric.to_html(index = True, header = True, bold_rows = True, justify = 'center')
    
    soupTable = BeautifulSoup(htmlTable,"lxml")
    
    for th in soupTable.find_all('tr')[0].find_all('th')[2:]:
        th['rowspan']='2'
    
    for th in soupTable.find_all('tr')[0].find_all('th')[2:6]:
        th['bgcolor']='#d3d2cd'
    
    for th in soupTable.find_all('tr')[0].find_all('th')[6:20]:
        th['bgcolor']='#94cffc'
    
    for th in soupTable.find_all('tr')[0].find_all('th')[20:]:
        th['bgcolor']='#fcbd94'
    
    
    #soupTable.table["width"]="100%"
    
    
    soupTable.find_all('tr')[0].find_all('th')[0].string = soupTable.find_all('tr')[1].find_all('th')[0].string
    soupTable.find_all('tr')[0].find_all('th')[0]['bgcolor']="#f4e8ad"
    soupTable.find_all('tr')[0].find_all('th')[1].string = soupTable.find_all('tr')[1].find_all('th')[1].string 
    soupTable.find_all('tr')[0].find_all('th')[1]['bgcolor']="#f4e8ad"
    soupTable.find_all('tr')[1].decompose()
    
    for tag in soupTable.select('th[valign="top"]'):
        tag["valign"]='middle'
        
    
    tag = soupTable.new_tag("style")
    tag.string = """ table, td, th {
        border: 1px solid black;
        font-size:11px;
    }
    html, body{
        height:100%;
        width:100%;
        padding:0;
        margin:0;
    }
    table {
        border-collapse: collapse;
    
        text-align: center;
    }"""
    soupTable.find('html').insert_before(tag) 
          
    with open(dir_path+"\\YBL.html", "w") as file:
        file.write(str(soupTable))    
    
    #posturl = 'https://portal.ant.amazon.com/sites/toc/bats/Shared%20Documents/NOC%20Metrics'
    #files = {'file': ('report.xls',open(dir_path+"\\YBL.html", 'rb'))}
    #r = requests.post(posturl, files=files)
    #r.text
    #driver = webdriver.Chrome(r'C:\Users\chenfa\Documents\webdriver\chromedriver')
    driver.get("https://portal.ant.amazon.com/sites/toc/bats/_layouts/Upload.aspx?List={2D442C0F-CE60-4871-A9F5-186D50684970}&RootFolder=https%3A%2F%2Fportal%2Eant%2Eamazon%2Ecom%2Fsites%2Ftoc%2Fbats%2FShared%20Documents%2FNOC%20Metrics")
    element = driver.find_element_by_id("ctl00_PlaceHolderMain_ctl01_ctl05_InputFile")
    element.send_keys(dir_path+"\\YBL.html")
    driver.find_element_by_id("ctl00_PlaceHolderMain_ctl00_RptControls_btnOK").click()
    time.sleep(5)
    driver.quit()
    return Matric
    
    
    
    
    
#import tkinter.simpledialog
#
#import xlwings as xw

def send_mail(isTls=True):
    send_from = ""
    password = ""

#    send_to = get_destination()
    #Email List for who will receive the alert 
    send_to = ['chenfa@amazon.com','yard-backlog-interest@amazon.com','neo-oo4@amazon.com']
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = 'yard-backlog-interest@amazon.com'
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = 'Yard Backlog Report - '+time.strftime("%p",time.localtime())
    sublist = Matric.loc['SortAR']['DaysTransBacklog[Today]'].tolist()+Matric.loc['SortTraditional']['DaysTransBacklog[Today]'].tolist()
    
    subnetwork = []
    for i in USFCd['SortAR']+USFCd['SortTraditional']:
        if i in USFCd['SortTraditional']:
            subnetwork.append('SortTraditional')
        elif i in USFCd['SortAR']:
            subnetwork.append('SortAR')
            
    transbacklog = pd.DataFrame(sublist,columns=['DaysTransBacklog[Today]'], index = pd.MultiIndex.from_arrays([subnetwork,USFCd['SortAR']+USFCd['SortTraditional']], names = ['NetWork','FCs'])).replace (0.00, np.nan)   
   
    def color_red(val):
        color = 'red' if val < 0.25 and type(val) == float else 'black'
        return 'color: %s' % color
    
    s = transbacklog.style.applymap(color_red, subset=['DaysTransBacklog[Today]'])

    msghtm = s.render()
#    msghtm = msghtm.replace('<td>','<td style="text-align: right;">')
    msghtm = msghtm.replace('nan','-')

# load the file

    soup = BeautifulSoup(msghtm,"lxml")

# create new text
    tag = soup.new_tag("p")
    tag.string = """Hello all, Please find the Yard Backlog Report attached, summary below: """
# insert it into the document
    soup.body.insert_before(tag)
#Styling     
    tag = soup.new_tag("style")
    tag.string = """ table, td, th {
                border: 1px solid black;
                font-size:11px;
                    }
        html, body{
                height:100%;
                width:100%;
                padding:0;
                margin:0;
                }
        table {
                border-collapse: collapse;

                text-align: center;
                }"""
    soup.find('html').insert_before(tag) 
    
    soup.find_all('th')[0]['class'] = soup.find_all('th')[3]['class']
    soup.find_all('th')[0].string = soup.find_all('th')[3].string
    soup.find_all('th')[1]['class'] = soup.find_all('th')[4]['class']
    soup.find_all('th')[1].string = soup.find_all('th')[4].string
    for x in soup.find_all('th')[3:6]:
        x.decompose() 
    for x in soup.find_all('th')[:3]:
        x['rowspan']="2"
# create new text
#    tag = soup.new_tag("p", style = "color:red;" )
#    tag.string = """Note: Starting from Jan 15, 2018, the yard backlog report will not be attached with this email , you can access the report by clicking the link below. """
# insert it into the document
#    soup.body.insert_before(tag)# create new text
#    tag = soup.new_tag("p")
#    tag.string = """https://portal.ant.amazon.com/sites/toc/bats/Shared%20Documents/NOC%20Metrics/Yard_Backlog_Report.pdf"""
    soup.body.insert_before(tag)# create new text
    tag = soup.new_tag("p")
    tag.string = """https://portal.ant.amazon.com/sites/toc/bats/Shared%20Documents/NOC%20Metrics/YBL.html"""
# insert it into the document
    soup.body.insert_before(tag)
    soup.table['border'] = "1"


    body = MIMEMultipart('alternative')  
#    body.attach(MIMEText("""Hello All,
#                  
#    
#                        Please find the Yard Backlog Report attached, summary below:  \n""" ))
    body.attach(MIMEText(str(transbacklog),'plain'))
    body.attach(MIMEText(soup, 'html'))
#    body = MIMEText(fp.read())
#    fp.close()
    msg.attach(body)
    
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(dir_path+"\\YBL.html","rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="YBL.html"')
    msg.attach(part)



    smtp = smtplib.SMTP('ballard.amazon.com', )
    if isTls:
        smtp.starttls()
    smtp.login(send_from,password)
    for address in send_to:
        smtp.sendmail(send_from, address, msg.as_string())
    smtp.quit()

#
def send_Alert(isTls=True):
    send_from = ""
    password = ""

#    send_to = get_destination()
    #Email List for who will receive the alert 
    send_to = ['neo-oo4@amazon.com']
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = 'neo-oo4@amazon.com'
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = 'Alter for YBL,Please check it! - '+time.strftime("%p",time.localtime())
   



    body = MIMEMultipart('alternative')  
    body.attach(MIMEText("""Hello All,
                  
    
                        YBL automation script has faced a problem, please run it mannually. """ ))

    msg.attach(body)
    


    smtp = smtplib.SMTP('ballard.amazon.com', )
    if isTls:
        smtp.starttls()
    smtp.login(send_from,password)
    for address in send_to:
        smtp.sendmail(send_from, address, msg.as_string())
    smtp.quit()

#
    

while True:
    print("Waiting...")
    if datetime.datetime.now().hour in (8,15) and datetime.datetime.now().minute == 1:
    #def read_web(webbook):
        try:
            driver = webdriver.Chrome(r'C:\Users\chenfa\Documents\webdriver\chromedriver')
            bg = read_bg(url[0])
            bgca = read_bg(url[1])
            cu = read_cu(url[2])
            cu = cu.reset_index(drop=True)
            cuca = read_cu(url[3])
            cuca = cuca.reset_index(drop=True)
            prius = priusData(USFC)
            priusca = priusCA() 
            wtp = wtpData()
            wtp = wtp[~wtp.index.isin(wtp[wtp['IM_flag'] == 'Unmanifested'].index)]
            InbPriPivot = inTridata()
            Matric = report()
            send_mail()
            print("Sent")
        except:
            send_Alert()
            
