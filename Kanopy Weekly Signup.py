#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
#import numpy as np
import datetime
import pandas as pd
import time
import os, sys, inspect
from time import gmtime, strftime
try:
    import MySQLdb
except:
    import pymysql as MySQLdb
from typing import Optional

def get_query_results(query: str,
                      server_host: str ='dbslave-2.kanopy.com',
                      db_user: str ='jupyter',
                      db_pass: str ='T@DN#^$5BPcYMDB+gB]y^q;2mZ|k',
                      db_name: str ='ks') -> Optional[pd.DataFrame]:

    df = None

    conn = MySQLdb.connect(host=server_host,
                           user=db_user,
                           passwd=db_pass,
                           db=db_name)

    df = pd.read_sql(query, conn)

    return df
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

def create_col(result,strr):
    temp_temp_day = 0    
    max_result2=result.shape[0]
    store = []
    week  = []
    sum_day = 0
    summ = 0
    k=int(result['created'].iloc[0])
    d=datetime.datetime.fromtimestamp(k).strftime('%Y-%m-%d %H:%M:%S')
    d = datetime.datetime.strptime(d,'%Y-%m-%d %H:%M:%S')    
    temp_temp_day  = (datetime.datetime.strftime(d,'%u'))
    for i in range(max_result2):
        k=int(result['created'].iloc[[i]])
        d=datetime.datetime.fromtimestamp(k).strftime('%Y-%m-%d %H:%M:%S')
        d = datetime.datetime.strptime(d,'%Y-%m-%d %H:%M:%S')    
        temp_day  = (datetime.datetime.strftime(d,'%u'))
        year_num  = (datetime.datetime.strftime(d,'%Y'))
        week_num  = (datetime.datetime.strftime(d,'%W'))
        week_num  = int(week_num)-1
        if(temp_day == temp_temp_day):
            sum_day =  sum_day + int(result[strr].iloc[[i]])
    
        else:
            final_day = sum_day
            sum_day = int(result[strr].iloc[[i]])
            if(int(temp_temp_day)%7 == 0):
                final = summ + final_day
                store.append(final)
                week.append(int(f'{year_num}' + f'{week_num}'))
                summ = 0
            else:
                summ = summ + final_day
            temp_temp_day = temp_day
    return store,week                         

result1 = get_query_results(query ='SELECT users.created,COUNT(DISTINCT authmap.uid) from ks_user_mapusers INNER JOIN authmap ON ks_user_mapusers.aid=authmap.aid INNER JOIN users ON users.uid=authmap.uid INNER JOIN ks_user_identity ON ks_user_identity.uid=users.uid WHERE users.created>=1491202800 AND users.created<=1533106799 AND ks_user_mapusers.provider=\"google\" AND ks_user_identity.domain_id not in (1, 13, 69, 74, 75, 76, 77, 78, 382, 569, 570, 571, 1103, 1535, 1976, 1992, 2030, 2502, 2641, 2982, 3065) GROUP BY users.created',
                      server_host ='dbslave-2.kanopy.com',          
                      db_user ='jupyter',
                      db_pass ='T@DN#^$5BPcYMDB+gB]y^q;2mZ|k',
                      db_name ='ks')
    
result2 = get_query_results(query ='SELECT users.created,COUNT(DISTINCT authmap.uid) from ks_user_mapusers INNER JOIN authmap ON ks_user_mapusers.aid=authmap.aid INNER JOIN users ON users.uid=authmap.uid INNER JOIN ks_user_identity ON ks_user_identity.uid=users.uid WHERE users.created>=1491202800 AND users.created<=1533106799 AND ks_user_mapusers.provider=\"facebook\" AND ks_user_identity.domain_id not in (1, 13, 69, 74, 75, 76, 77, 78, 382, 569, 570, 571, 1103, 1535, 1976, 1992, 2030, 2502, 2641, 2982, 3065) GROUP BY users.created',
                      server_host ='dbslave-2.kanopy.com',          
                      db_user ='jupyter',
                      db_pass ='T@DN#^$5BPcYMDB+gB]y^q;2mZ|k',
                      db_name ='ks')    
    
result3 = get_query_results(query ='SELECT users.created,COUNT(users.uid) from users INNER JOIN ks_user_identity ON users.uid=ks_user_identity.uid WHERE users.created>=1491202800 AND users.created<=1533106799 AND ks_user_identity.domain_id not in (1, 13, 69, 74, 75, 76, 77, 78, 382, 569, 570, 571, 1103, 1535, 1976, 1992, 2030, 2502, 2641, 2982, 3065) GROUP BY users.created',
                      server_host ='dbslave-2.kanopy.com',          
                      db_user ='jupyter',
                      db_pass ='T@DN#^$5BPcYMDB+gB]y^q;2mZ|k',
                      db_name ='ks')

result4 = get_query_results(query ='SELECT users.created,COUNT(users.uid) from users WHERE users.created>=1491202800 AND users.created<=1533106799 AND users.status=1 GROUP BY users.created',
                      server_host ='dbslave-2.kanopy.com',          
                      db_user ='jupyter',
                      db_pass ='T@DN#^$5BPcYMDB+gB]y^q;2mZ|k',
                      db_name ='ks')    
         
store_GO,week        = create_col(result1,"COUNT(DISTINCT authmap.uid)")
store_FB,week         = create_col(result2,"COUNT(DISTINCT authmap.uid)")   
store_SU,week         = create_col(result3,"COUNT(users.uid)")
store_Ver_EM,week         = create_col(result4,"COUNT(users.uid)")
store_EM             = [x+y for x,y in zip(store_FB,store_GO)]
store_EM             = [x-y for x,y in zip(store_SU,store_EM)]
store_GO_Per         = [(z/x)*100 for x,z in zip(store_SU,store_GO)]
store_FB_Per         = [(y/x)*100 for x,y in zip(store_SU,store_FB)]
store_EM_Per         = [(a/x)*100 for a,x in zip(store_EM,store_SU)] 
store_Ver_EM_Per          = [(b/x)*100 for b,x in zip(store_Ver_EM,store_SU)] 
store_Not_Ver_EM          = [(x-b) for b,x in zip(store_Ver_EM,store_SU)] 
store_Not_Ver_EM_Per      = [(c/a)*100 for a,c in zip(store_EM,store_Not_Ver_EM)]

week                 = ['Year-Week']+week
store_GO             = ['No. of Google Singups']+store_GO
store_FB             = ['No. of FB Signups']+store_FB
store_SU             = ['Total Signups']+store_SU         
store_Ver_EM         = ['Verified Email Users']+store_Ver_EM
store_EM             = ['Email Signups']+store_EM
store_GO_Per         = ['Google Singups %']+store_GO_Per
store_FB_Per         = ['FB Singups %']+store_FB_Per              
store_EM_Per         = ['Email Singups %']+store_EM_Per
store_Ver_EM_Per     = ['Verified Email Users %']+store_Ver_EM_Per
store_Not_Ver_EM     = ['Not Verified Email Users']+store_Not_Ver_EM                 
store_Not_Ver_EM_Per = ['Not Verified Email Users %']+store_Not_Ver_EM_Per                

Masterlist=zip(week,store_GO,store_FB,,store_EM,store_SU,store_GO_Per,store_FB_Per,store_EM_Per,store_Ver_EM,store_Ver_EM_Per,store_Not_Ver_EM,store_Not_Ver_EM_Per)

                
with open('yyy.csv', 'w') as csvfile1:
    w = csv.writer(csvfile1)
    w.writerows(Masterlist)