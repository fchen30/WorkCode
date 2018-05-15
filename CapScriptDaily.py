# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 12:19:57 2018

@author: chenfa
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 09:40:02 2018

@author: chenfa
"""

import pandas as pd
import datetime
import numpy as np
import tkinter
import re
import math

#Starting Date and length




def get_Cap_Data():
#Get Date for yesterday.
#    pkg=[]
#    pkg = [datetime.datetime.now().date()- datetime.timedelta(days = 1),2]
    pkg=[]
    pkg = [datetime.datetime.now().date()- datetime.timedelta(days = 1)]
    
    #Input dates
    #def get_Date():  
    #
    #    tk_root = tkinter.Tk()
    #    tk_root.title('Please input date range information')
    #    tkinter.Label(tk_root, text="Starting Year", padx = 2).grid(row=1, sticky = 'w')
    #    tkinter.Label(tk_root, text="Starting Month",padx = 2).grid(row=2, sticky = 'w')
    #    tkinter.Label(tk_root, text="Starting Day",padx = 2).grid(row=3, sticky = 'w')
    #    tkinter.Label(tk_root, text="How Many days ago from this date? ",padx = 2).grid(row=4)
    #    
    #    eY=tkinter.Entry(tk_root)
    #    eY.focus_set()
    #    eM=tkinter.Entry(tk_root)
    #    eD=tkinter.Entry(tk_root)
    #    eA=tkinter.Entry(tk_root)
    #    
    #    eY.grid(row =1, column =1)
    #    eM.grid(row =2, column =1)
    #    eD.grid(row =3, column =1)
    #    eA.grid(row =4, column =1)
    #
    #    def submit():
    #        def check(x):
    #          if int(x):
    #              
    #            if len(x) <= 2:
    #                if len(x)==1:
    #                    return '0'+x
    #                else:
    #                    return x
    #            else:
    #               tkinter.messagebox.showwarning("Warning", "Check your input")
    #               tk_root.quit()
    #               tk_root.destroy() 
    #               get_Date()
    #          else:              
    #              tkinter.messagebox.showwarning("Warning", "Check your input")
    #              tk_root.quit()
    #              tk_root.destroy() 
    #              get_Date()
    #              
    #        Year = eY.get()
    #        Month = check(eM.get())
    #        Day = check(eD.get())
    #        print (Year+'-'+Month+'-'+Day)
    #        pkg.append(Year+'-'+Month+'-'+Day)
    #        pkg.append(eA.get())
    #
    #
    #    def close():
    #        tk_root.quit()
    #        tk_root.destroy()        
    #
    #    
    #    bs = tkinter.Button(tk_root, text = "Submit",width = 10, command = submit)
    #    bs.grid(row = 5, column = 0)
    #    bc = tkinter.Button(tk_root, text = "Close", width = 10, command = close)
    #    bc.grid(row = 5, column = 1)
    #    tkinter.mainloop()
    #
    #    
    #get_Date()
    
    #Get Yesterday's data
    try:
        print(pkg[0])
        firstExcel = pd.read_excel("https://atrops-web-na.amazon.com/caps/show/NA-"+pkg[0].strftime("%Y-%m-%d")+".xls")
    except:
        print('Cant load data from Atrops')
    
    #For loop if use input function
    #for i in range(1,int(pkg[1])+1):
    #For loop if not ask user to input
#    for i in range(1,pkg[1]+1):
##    Get past x days' data
#    #    print(datetime.datetime.strptime(pkg[0],"%Y-%m-%d")-datetime.timedelta(days =i))
#        print(pkg[0]-datetime.timedelta(days =i))
#    #    firstExcel = pd.concat([firstExcel,pd.read_excel("https://atrops-web-na.amazon.com/caps/show/NA-"+(datetime.datetime.strptime(pkg[0],"%Y-%m-%d")-datetime.timedelta(days =i)).strftime("%Y-%m-%d")+".xls")])
#        firstExcel = pd.concat([firstExcel,pd.read_excel("https://atrops-web-na.amazon.com/caps/show/NA-"+(pkg[0]-datetime.timedelta(days =i)).strftime("%Y-%m-%d")+".xls")])
#    
#    
    ###Deleting all rows if tis schedule cap hit column is null
    firstExcel = firstExcel[~firstExcel['Schedule Cap Hit'].isnull()].reset_index(drop=True)
    
    #TimeZone = ["PST", "PDT", "MST", "MDT", "CST", "CDT", "EST", "EDT", "HST", "SGT"]   
    
    #def cleanDate(col):
    #    TimeZone = ["PST", "PDT", "MST", "MDT", "CST", "CDT", "EST", "EDT", "HST", "SGT"]
    #    for tz in TimeZone:
    #        print(tz)
    #        if pd.isnull(col):
    #            print('NA')
    #            pass
    #        else:
    #            if type(col)==str and len(col)>8:
    #                print(col)
    #                if re.search("("+tz+")",col):
    #                    print(col)
    #                    print(col[:len(col)-4].lstrip())
    #                    return col[:len(col)-4].lstrip()
    #                
    #            else:
    #                return col
    
    
      
    
    
    #Delete time zone notation for all dates 
    try:    
        def cleanDate(col):
            pattern = re.compile("^[0-9]{4,}-[0-9]{2,}-[0-9]{2,}\s[0-9].:[0-9]{2,}\s...")
            if pd.isnull(col):
                print('NA')
                return np.nan
            else:
                if pattern.match(col):
                    print(col)
                    return col[:len(col)-4].lstrip()
                else:
                    return col
        
        for i in firstExcel.columns[-10:]:
            if re.search("(CPT)", i) or re.search("(Cap)", i):
                firstExcel[i] = firstExcel.apply(lambda x: cleanDate(x[i]), axis = 1)
                    
        
        #DayShiftStart =  datetime.datetime.strptime("7:00","%H:%M") 
        #DayShiftEnd = datetime.datetime.strptime("18:00","%H:%M") 
                
        ###All MVC lane        
        MVC = pd.read_csv(r"C:\Users\chenfa\Documents\CapScrape\MVC.csv")
        
        ####All VendorFlex Lane
        VendorFlex = pd.read_csv(r"C:\Users\chenfa\Documents\CapScrape\VendorFlex.csv")
        VendorFlex = VendorFlex.assign(VendorFlex = pd.Series('Yes', index = VendorFlex.index).values)
        Pantry = ["CVG7","MCI7","ONT7","OAK7","EWR7","DFW9" ]
        SpecialHandling = ["AVP3","ATL8","BFI7","FTW2","MDW8"]
        #Get Rows that are not in the Filter, which are not our targets  
        #excel = firstExcel
        
        
        #### concatenate FC and Sort Code for regular lanes
        def Lane(x):
            x = x.fillna(np.nan)
            if len(str(x['Sort Code'])) == 4:
                return x['FC']+'->'+str(x['Sort Code'])
            elif len(str(x['Dest Warehouse'])) == 4:
                return x['FC']+'->'+str(x['Dest Warehouse'])
            else:
                return np.nan
        ###Testing Macthes 
        #excel['Lane'] = excel.apply(lambda x: Lane(x), axis =1)
        #
        #def match(x):
        #    return x in MVC['Lane'].tolist()
        #len(list(filter(match, excel['Lane'])))
        #len(excel['Lane'])
                
        ####Define Match function to look up value in the Mark column in compareTo table based on Key and join the result to the Left Table    
        def match(left, compareTo, keyList, mark):
            return left.merge(compareTo, how = 'left', on = keyList )
        #    not_in = left.merge(compareTo, how = 'left', on = keyList )
        #    return left[left.index.isin(not_in[pd.isnull(not_in[mark])].index)]
        ####Yes it is a MVC lane
        metric = match(firstExcel,MVC,['FC', 'Sort Code'],'MVC')
        ####No it is not a MVC lane
        metric['MVC'] = metric['MVC'].fillna('No')
        
        metric.fillna(np.nan)
        ###rename column names for uploding to redshift table 
        new_columns = metric.columns.values
        new_columns[10] = 'CPT PDT'
        new_columns[12] = 'Previous CPT PDT'
        new_columns[15]='SLAM Cap Hit Time PDT'
        new_columns[16] = 'SLAM Cap Hit Time Local'
        new_columns[18] ='Schedule Cap Hit Time PDT'
        new_columns[19] ='Schedule Cap Hit Time Local'
        metric.columns = new_columns
        
        #
        #not_in = firstExcel.merge(MVC, how = 'left', on = ['FC', 'Sort Code'] )
        #metric = firstExcel[firstExcel.index.isin(not_in[pd.isnull(not_in['Lane'])].index)]
        
        
        ####Use shipmethod, sortcode, and cpt time as PK
        def Carrier_Index(df):
        #    try:
        #        if df['Ship Method'].isnull() or df['Sort Code'].isnull():
            return pd.Series([str(df['FC'])+str(df['Ship Method'])+ str(df['Sort Code'])+str(df['CPT PDT'])+str(df['Cap Type'])+str(df['UOM']+str(df['SLAM Volume At CPT']))])
        #    except:
        #        print(sys.exc_info()[0])
        #
        metric['Carrier_Index'] = metric.apply(lambda x: Carrier_Index(x), axis =1)
        
        metric['Lane']= metric.apply(lambda x: Lane(x), axis =1)
        
        metric = metric.reset_index(drop=True)
        
        metric = match(metric, VendorFlex, ['FC'], 'VendorFlex')
        metric = metric.reset_index(drop=True)
        
        
        ###VendorFlex?
        def VendorFlexMark(x):
            if x['VendorFlex'] != 'Yes':
                if x['FC'][0] =='V': 
                    return 'Yes'
                else:
                    return 'No'
            else: 
                return 'Yes'
            
        metric['VendorFlex'] = metric.apply(lambda x: VendorFlexMark(x), axis =1)    
            
        #metric = metric[metric['VendorFlex']=='No'].reset_index(drop=True)
        
        
        ####Pantry?
        def isPantry(x,y): 
           if x['FC'][0] in y:
               return 'Yes'
           else: 
               return 'No'
        
        metric['Pantry'] = metric.apply(lambda x: isPantry(x,Pantry), axis =1)   
        metric['SpecialHandeling'] = metric.apply(lambda x: isPantry(x,SpecialHandling), axis =1)   
        
        
        #Sort Center?
        def isSortCenter(x,col):   
            if pd.isnull(x[col]):
                return x[col]
            else:
                if re.search("SORTCENTER", x[col]):
                    return "Sort Center"
                elif re.search("DS", x[col]):
                    return "Delivery Station"
                else: 
                    return np.nan
                
        metric['SortCenter'] = metric.apply(lambda x: isSortCenter(x,'Ship Method'), axis =1)     
        
        
        
        def get_amazon_week( date ):
            # observe that the Amazon week always starts one day before the iso week.
            # getting the isoweek for the next day will give us the Amazon week.
            nextday = date + datetime.timedelta(days=1)
            (isoyear, isoweek, isoday) = nextday.isocalendar()
            return (isoyear, isoweek)
        # 
        #def amz_year_start(year):
        #    # Jan 4th is always in the first ISO week of the year.
        #    fourth_jan = datetime.date(year, 1, 4)
        #    # The amazon week is same as the iso week, just starts on Sunday instead of
        #    # Monday.
        #    # Python has an isoweekday function that returns the iso day number 1 to 7
        #    # By subtracting that number from the iso date we end up one day before the
        #    # start of that iso week which is the start of the amazon week
        #    delta = datetime.timedelta(fourth_jan.isoweekday())
        #    return fourth_jan - delta
        # 
        #def get_amazon_week_date_range( year, week):
        #    # Get the first day of the amazon year
        #    year_start = amz_year_start(year)
        #    # Add the number of weeks in days (weeks start counting at 1 not 0)
        #    week_start = year_start + datetime.timedelta((week-1) * 7)
        #    # The end of the range is 7 days later.
        #    week_end = week_start + datetime.timedelta(7)
        #    return (week_start, week_end)
        #
        metric['weeknumber'] = metric.apply(lambda x:get_amazon_week(datetime.datetime.strptime(x['CPT PDT'],"%Y-%m-%d %H:%M"))[1],axis =1)
        
        
        def getdate(time):
            pattern = re.compile("^[0-9]{4,}-[0-9]{2,}-[0-9]{2,}")
            if pattern.match(time[:10]):
                return time[:10]
            else:
                pass
        def capdate(cap,cpt):
            if cap == 'n/a' or  pd.isnull(cap):
                return getdate(cpt)
            else:
                return getdate(cap)
        
        metric['Scheduled Cap Hit Date PDT'] = metric.apply(lambda x:capdate(x['Schedule Cap Hit Time PDT'],x['CPT PDT']),axis =1)
        metric['CPT Date PDT'] =  metric.apply(lambda x:getdate(x['CPT PDT']),axis =1)
        metric['CPT_Date_Local'] =  metric.apply(lambda x:getdate(x['CPT Local']),axis =1)  
        
        def getTime(time):
            pattern = re.compile("^[0-9].:[0-9]{2,}")
            if pattern.match(time[-5:]):
                return time[-5:]
            else:
                pass
        
        metric['CPT Time PDT'] =  metric.apply(lambda x:getTime(x['CPT PDT']),axis =1)      
        metric['CPT Time Local'] = metric.apply(lambda x:getTime(x['CPT Local']),axis =1)  
        #def capIndex(x):
        #    x = x.fillna('')
        #    return x['FC']+x['Ship Method']+x['Dest Warehouse']+x['Ship Option Group Name']
        #
        #firstExcel['Cap_Index'] = firstExcel.apply(lambda x: capIndex(x), axis =1)
        #
        #
        #metic = firstExcel.merge(FCref.iloc[:,:4], how = 'left', on = ['FC'] ).set_index('FC', inplace = False)
        #metic = metic.fillna('')
        #
        #
        #def lookup(lookUpValue, SearchIn, ValueFrom):
        #    print(lookUpValue)
        #    try:
        #        index = SearchIn.index[SearchIn == lookUpValue].values[0]
        #        return ValueFrom[index]
        #    except: 
        #        return 0
        #    
        #
        
        
        ###SLA is 3 hours. There is no change can be made 3 hours before the CPT 
        metric['CPTminusPickSLA'] = metric['CPT PDT'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M")-datetime.timedelta(hours = 3))
        
        
        #Difference between the Cap Hit Tme and the CPT is the Capped Time  
        def capHours(cpt,caphit):
            if caphit =="n/a"  or  pd.isnull(caphit) :
                return 0
            elif type(cpt) == str:
                return round((datetime.datetime.strptime(cpt,"%Y-%m-%d %H:%M") - datetime.datetime.strptime(caphit,"%Y-%m-%d %H:%M")).total_seconds()/3600,2)
            else:
                if cpt<datetime.datetime.strptime(caphit,"%Y-%m-%d %H:%M"):
                    return 0
                else:
                    return round((cpt - datetime.datetime.strptime(caphit,"%Y-%m-%d %H:%M")).total_seconds()/3600,2)
        
        metric['CAP_HOURS_Sch'] = metric.apply(lambda x: capHours(x['CPTminusPickSLA'],x['Schedule Cap Hit Time PDT']), axis =1)
        metric['CAP_HOURS_Sch_SLA']= metric.apply(lambda x: capHours(x['CPT PDT'],x['Schedule Cap Hit Time PDT']), axis =1)
        
        # Slam Capped Hour
        def CapHrSlam(hit,cpt,caphit):
            if hit == 'No':
                return 0
            else:
                return capHours(cpt, caphit)
        
        metric['CAP HOURS_SLAM']= metric.apply(lambda x: CapHrSlam(x['SLAM Cap Hit'],x['CPT PDT'],x['SLAM Cap Hit Time PDT']), axis =1)
        
        
        # Percentage of capped time with in a shipping cycle 
        def notCapPercent(cpt, caphit,preCpt):    
            if caphit =="n/a" or pd.isnull(caphit):
                return np.nan
            else:
                cpt = datetime.datetime.strptime(cpt,"%Y-%m-%d %H:%M")
                caphit = datetime.datetime.strptime(caphit,"%Y-%m-%d %H:%M")
                preCpt = datetime.datetime.strptime(preCpt,"%Y-%m-%d %H:%M")
                return 1-round((cpt - caphit).total_seconds()/(cpt - preCpt).total_seconds(),2)
        
        def segmanet(col):
            if pd.isnull(col):
                return np.nan
            elif col < 0.245:
                return '1st Qtr'
            elif col>=0.745:
                return '4th Qtr'
            elif col>=0.245 and col<0.495:
                return '2nd Qtr'
            else:
                return '3rd Qtr'
            
        metric['Percent of nonCapped_Sch']= metric.apply(lambda x: notCapPercent(x['CPT PDT'],x['Schedule Cap Hit Time PDT'],x['Previous CPT PDT']), axis =1)
        metric['Cap_Sch_Segement'] = metric.apply(lambda x: segmanet(x['Percent of nonCapped_Sch']), axis =1)
        metric['Percent of nonCapped_SLAM']= metric.apply(lambda x: notCapPercent(x['CPT PDT'],x['SLAM Cap Hit Time PDT'],x['Previous CPT PDT']), axis =1)
        metric['Cap_SLAM_Segement'] = metric.apply(lambda x: segmanet(x['Percent of nonCapped_SLAM']), axis =1)
        
        #Rename Columns Header for loading to database.
        columns = metric.columns.values
        new_columns = []
        for i in columns:
             new_columns.append(i.replace(' ','_'))
        
        metric.columns = new_columns
        #metric = metric.replace([' ',''], np.nan, regex=True)
        
        #Replace NaN to None for loading to Database 
        metric = metric.where((pd.notnull(metric)),other = None)
    except:
        print('Data Transformation error')
    #Save as CSV file 
    try:
        metric.to_csv(r"C:\Users\chenfa\Documents\CapScrape\capped_hour.csv",index = False)
    except:
        print('Cant not save as CSV file')
#from collections import OrderedDict


##Redshift and AWS connection 

#createTalbe = print(pd.io.sql.get_schema(metric,'public.Capped_Hour_Table'))
#####Create Table, only need to run once 
#createTable ="""
#  CREATE TABLE public.Capped_Hour_Table (
#  "FC" TEXT,
#  "Ship_Method" TEXT,
#  "Sort_Code" TEXT,
#  "Dest_Warehouse" TEXT,
#  "Ship_Option_Group_Name" TEXT,
#  "Cap_Type" TEXT,
#  "Cap_Value" INTEGER,
#  "SLAM_Volume_At_CPT" DECIMAL(10,2),
#  "Schedule_Volume_At_CPT" DECIMAL(10,2),
#  "UOM" TEXT,
#  "CPT_PDT" TIMESTAMP,
#  "CPT_Local" TIMESTAMP,
#  "Previous_CPT_PDT" TIMESTAMP,
#  "Previous_CPT_Local" TIMESTAMP,
#  "SLAM_Cap_Hit" TEXT,
#  "SLAM_Cap_Hit_Time_PDT" TIMESTAMP,
#  "SLAM_Cap_Hit_Time_Local" TIMESTAMP,
#  "Schedule_Cap_Hit" TEXT,
#  "Schedule_Cap_Hit_Time_PDT" TIMESTAMP,
#  "Schedule_Cap_Hit_Time_Local" TIMESTAMP,
#  "MVC" TEXT,
#  "Carrier_Index" TEXT,
#  "Lane" TEXT,
#  "VendorFlex" TEXT,
#  "Pantry" TEXT,
#  "SpecialHandeling" TEXT,
#  "SortCenter" TEXT,
#  "weeknumber" INTEGER,
#  "Scheduled_Cap_Hit_Date_PDT" DATE,
#  "CPT_Date_PDT" DATE,
#  "CPT_Date_Local" DATE,
#  "CPT_Time_PDT" TEXT,
#  "CPT_Time_Local" TEXT,
#  "CPTminusPickSLA" TIMESTAMP,
#  "CAP_HOURS_Sch" DECIMAL(10,2),
#  "CAP_HOURS_Sch_SLA" DECIMAL(10,2),
#  "CAP_HOURS_SLAM" DECIMAL(10,2),
#  "Percent_of_nonCapped_Sch" DECIMAL(10,2),
#  "Cap_Sch_Segement" TEXT,
#  "Percent_of_nonCapped_SLAM" DECIMAL(10,2),
#  "Cap_SLAM_Segement" TEXT,
#  Primary key(Carrier_Index,FC, Lane))
#  compound sortkey(CPT_PDT, CPT_LOCAL);    
#)"""


######Define DataType for Columns 
#def dataType(val, current_type):
#    try:
#        # Evaluates numbers to an appropriate type, and strings an error
#        t = ast.literal_eval(val)
#    except ValueError:
#        return 'varchar'
#    except SyntaxError:
#        return 'varchar'
#    if type(t) in [int, float]:
#        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
#           # Use smallest possible int type
#           if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
#               return 'smallint'
#           elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
#               return 'int'
#           else:
#               return 'bigint'
#        if type(t) is float and current_type not in ['varchar']:
#           return 'decimal'
#        else:
#           return 'varchar'


#
#SQLALCHEMY
#url = sqlalchemy.engine.url.URL(drivername='redshift+psycopg2',
#            username='gundam',
#            password='BandaiJapan1',
#            host='neophx1.c2nmc4beb1ui.us-east-1.redshift.amazonaws.com',
#            port='8192',
#            database='neophx')
#
#con=sqlalchemy.create_engine(url)
### 
##connection = con.connect()
#connection.execute(createTable)
#print(connection.execute('DROP TABLE public.Capped_Hour_Table'))
#connection.close()


#cur.execute("""delete from "public.Capped_Hour_Table" where Carrier_Index is null """)
#cur.execute(createTable)
#cur.execute('DROP TABLE public.Capped_Hour_Table')
#cur.execute('DROP TABLE Capped_Hour_Table')

#####Inset statement for all rows of pandas dataframe(Extremly slow performance)
#value=[]
#for row in metric.iloc[0:].iterrows():
#    con=psycopg2.connect(dbname= 'neophx', host='neophx1.db.amazon.com', 
#    port= '8192', user= 'gundam', password= 'BandaiJapan1')
#    cur = con.cursor()
##  value.append(tuple(row[1]))
#
##values = ", ".join(map(str,value))    
##sql = """
##        INSERT INTO "public.Capped_Hour_Table"
##        (FC, Ship_Method, Sort_Code, Dest_Warehouse,Ship_Option_Group_Name,Cap_Type,Cap_Value,SLAM_Volume_At_CPT, Schedule_Volume_At_CPT, UOM ,CPT_PDT,	CPT_Local,	Previous_CPT_PDT,	Previous_CPT_Local,	SLAM_Cap_Hit,	SLAM_Cap_Hit_Time_PDT,	SLAM_Cap_Hit_Time_Local,	Schedule_Cap_Hit,	Schedule_Cap_Hit_Time_PDT,	Schedule_Cap_Hit_Time_Local,	MVC,	Carrier_Index,	Lane,	VendorFlex,	Pantry,	SpecialHandeling,	SortCenter,	weeknumber,	Scheduled_Cap_Hit_Date_PDT, CPT_Date_PDT,	CPT_Time_PDT,	CPT_Time_Local, CPTminusPickSLA, CAP_HOURS_Sch, CAP_HOURS_Sch_SLA,CAP_HOURS_SLAM,Percent_of_nonCapped_Sch,Cap_Sch_Segement,Percent_of_nonCapped_SLAM,Cap_SLAM_Segement) 
##        VALUES{};""".format(values)
#    cur.execute("""
#            INSERT INTO "public.Capped_Hour_Table"
#            (FC, Ship_Method, Sort_Code, Dest_Warehouse,Ship_Option_Group_Name,Cap_Type,Cap_Value,SLAM_Volume_At_CPT, Schedule_Volume_At_CPT, UOM ,CPT_PDT,	CPT_Local,	Previous_CPT_PDT,	Previous_CPT_Local,	SLAM_Cap_Hit,	SLAM_Cap_Hit_Time_PDT,	SLAM_Cap_Hit_Time_Local,	Schedule_Cap_Hit,	Schedule_Cap_Hit_Time_PDT,	Schedule_Cap_Hit_Time_Local,	MVC,	Carrier_Index,	Lane,	VendorFlex,	Pantry,	SpecialHandeling,	SortCenter,	weeknumber,	Scheduled_Cap_Hit_Date_PDT, CPT_Date_PDT,	CPT_Time_PDT,	CPT_Time_Local, CPTminusPickSLA, CAP_HOURS_Sch, CAP_HOURS_Sch_SLA,CAP_HOURS_SLAM,Percent_of_nonCapped_Sch,Cap_Sch_Segement,Percent_of_nonCapped_SLAM,Cap_SLAM_Segement) 
#            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",tuple(row[1]))
##            (row[1][0],row[1][1],row[1][2],row[1][3],row[1][4],row[1][5],row[1][6],row[1][7],row[1][8],row[1][9],row[1][10],row[1][11],row[1][12],row[1][13],row[1][14],row[1][15],row[1][16],row[1]17],row[1]18],row[1][19],row[1][20],row[1][21],row[1][22],row[1][23],row[1][24],row[1][25],row[1][26],row[1][27],row[1][28],row[1][29],row[1][30],row[1][31],row[1][32],row[1][33],row[1][34],row[1][35],row[1][36],row[1][37],row[1][38],row[1][39]))
#    con.commit()
##cur.execute(sql)
#
#cur.close()


def updateDatabase():
###Use S3 to import data into Redshift is a much faster way 
    import ast
    import psycopg2
    import sqlalchemy
    import sqlite3
    #Psycopg to connect with RedShift 
    con=psycopg2.connect(dbname host \
    port=, user= , password=)
#    cur = con.cursor()
    import boto3
    
    #### Connect to S3
    try:
        s3 = boto3.client('s3')
    except:
        print('Cannot connect to S3')
    ####Build bucket, only run once     
    #s3.create_bucket(Bucket='chenfabucket')
    
    
    ###Upload file to S3
    filename = r"C:\Users\chenfa\Documents\CapScrape\capped_hour.csv"
    myBucket = 'chenfabucket'
    try:
        s3.upload_file(filename,myBucket,'capped_hour')
        print('Upload Successful')
    except:
        print('Cannot upload to S3')
    #####Check all exsiting files inside the bucket 
    #s3r = boto3.resource('s3')
    #my_bucket = s3r.Bucket('chenfabucket')
    #for objects in my_bucket.objects.all():
    #    print(objects.key)
    
    
    ####File path for the uploaded csv file inside the S3 bucket      
    file_path = 's3://{}/{}'.format(myBucket,'capped_hour')
    
    
    ###SQL statement of copy file from S3 to Redshift Table 
    schema = 'neo'
    table = 'Capped_Hour_Table'
    aws_access_key_id =
    aws_secret_access_key 
    sql="""copy {}.{} from '{}'\
            credentials \
            'aws_access_key_id={};aws_secret_access_key={}' \
            csv \
            ignoreheader 1; """\
            .format(schema, table, file_path, aws_access_key_id, aws_secret_access_key)
#    sqlerror = """
#    select query, filename,line_number as line, 
#    colname, type, position, raw_line as line_text,
#    raw_field_value as field_text, 
#    err_reason as reason
#    from stl_load_errors 
#    order by query desc
#    limit 10;"""
#    cur.execute(sqlerror)
            
    ####Execute SQL Statement  
    try:       
        cur = con.cursor()        
        print('Connected to Database')
    except:
        print('Connection failed')
    try:
        cur.execute(sql) 
        con.commit()
        print('Update the table successfully')
    except:
        print('Failed to update the table')
    cur.close()    


while True:
    print("Waiting...")
    if datetime.datetime.now().hour == 7 and datetime.datetime.now().minute == 1:
        get_Cap_Data()
        updateDatabase()
           

#cur.fetchone()


#sqlite3
#con = sqlite3.connect(":memory:" )
#cur = con.cursor()
#cur.execute(createTable)
#
#
#metric.to_sql('Capped_Hour_Table',con,schema = 'public', index = False, if_exists='append')
#n=metric.iloc[0:1]
#n.to_sql(name ='public.Capped_Hour_Table',con = con, if_exists ='append', index = False)
#

#
#def reviewLevel(lookup, x):
#    if x['FC_type'] == 'Vender Flex':
#        return 'Not OB Trans Execution'
#    else:
#        if x['UOM'] == 'CUBIC_FEET':
#            return 'Linehaul Cubic Caps'
#        else :
#            return lookup(x['Carrier Index'],Carrierrf['Index'],Carrierrf['Review Level'])
# 
#metic['Review_Level'] = metic.apply(lambda x: reviewLevel(lookup, x), axis =1)
#
#
#def sortcenter(lookup, x):
#    if lookup(x['Carrier Index'],Carrierrf['Index'], Carrierrf['Sort Center']) == 0:
#        return ' '
#    else:
#        return lookup(x['Carrier Index'], Carrierrf['Index'], Carrierrf['Sort Center'])
#    
#metic['SortCenter'] = metic.apply(lambda x: sortcenter(lookup, x), axis =1)    
#
#metic = metic[metic['SortCenter']=='Sort Center']
#
#
#
#def carrierGroup(lookup, x):
#    if x['FC_type']=="Vendor Flex":
#        return x['FC_type']
#    elif x['SortCenter'] == "Sort Center" and x['UOM']=="CUBIC_FEET" and re.search("AMTRAN",x['Ship Method']):
#        return "Sort Center - Lane Cubic Caps"
#    else:
#        return lookup(x['Carrier Index'],Carrierrf['Index'],Carrierrf['Carrier Group'])
#    
#metic['CarrierGroup'] = metic.apply(lambda x: carrierGroup(lookup, x), axis =1)    



#metic.groupby()
#def costIimpact():




##    
#x = 'deed'
#
#
#for i in range(0,int(len(x)/2)):
#    if x[i] == x[int(len(x)-i)-1]:
#        print('Match')
#

#x = pd.read_csv(r'C:\Users\chenfa\Downloads\Copy of Book1.csv') 
#
#with open(r'C:\Users\chenfa\Downloads\list.txt','w') as thefile:
#    for item in x['tracking_id'].tolist():
#        thefile.write("'%s',\n" % item)
#    
  