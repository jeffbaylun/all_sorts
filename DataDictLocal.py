# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 22:53:51 2022
@author: Nicholas Krauss
"""

import pandas as pd
import numpy as np
import re 
import time
from Logging_Class import Logging
from tabulate import tabulate
import os
from getpass import getuser
from datetime import datetime as dt


datestring = dt.strftime(dt.now(),'%Y%m%d_%H%M%S')

log = Logging(task='DataDictDoneDirtCheap', 
              folder=fr'C:\Users\{getuser()}\OneDrive\Documents', 
              filename=f'Der_Log_{datestring}.log',
              log_print=True)
log.logging_folder()
os.chdir(log.folder)



def query(conn_obj, sql_syntax):
    try:
        t = time.perf_counter()
        conn  = conn_obj
        data = pd.read_sql(sql_syntax,conn).astype(object)
        elapsed = round((time.perf_counter()-t)/60,2)
        print(f'{len(data)},{elapsed}')
        log.logging_func('I', f'\n***SQL SYNTAX START***\n\t{sql_syntax}\n***SQL SYNTAX END***\n[ROW COUNT]:{len(data)},[ELAPSED]:{elapsed}')
    except Exception as e:        
        log.logging_func('E',f'{e}')
    finally:
        if conn is not None:
            conn.close()
    return data 
################################################################################

def view_to_table(view_table, objects):
    global a,b,c
    try:
        details = []
        for index, row in view_table.iterrows():
            #########################################
            try:
                a = row['index']
                b = ' '.join(str(row['view_definition']).split()).replace('(|)',' ') 
                c = row['table_name']
                match_objects = '~'.join(set(re.findall(objects, b)))
                complete = pd.DataFrame(data=[match_objects], columns=['obj_name'])
                complete = complete['obj_name'].str.split('~',expand=True)
                complete.insert(0,'index',a)
                complete = complete.melt(id_vars=['index'])
                details.append(complete)
            except Exception as e:
                log.logging_func('E',f'{e}')
            ###############################################
    except Exception as e:
        log.logging_func('E',f'{e}')
    finally:
        total = pd.concat(details)
        total = total.replace(r'^\s*$',np.nan, regex=True)
        total = total[total['value'].notna()].drop_duplicates()
        total = total.merge(view_table, how ='inner', left_on='index', right_index=True)
    return total



#############################################################

#example
# import sqlite3 as sql
# import random 
# import pandas as pd
# from faker import Faker
# import string

# try:
#     conn = sql.connect(f'fake_database_{datestring}.db')
#     cursor = conn.cursor()
    
#     fake = Faker()
#     table_names  = pd.Series(list(set([fake.city() for i in range(200)]))[:625]).str.replace('\s','',regex=True).str.lower()
#     S = 10
#     column_names = pd.Series(['COLUMN_'+(',COLUMN_'.join(set(random.choices(string.ascii_uppercase, k = S)))) for i in range(500)]) 
#     for a, b in zip(table_names, column_names):
#         create_statement = f'CREATE TABLE {a} ({b})'
#         cursor.execute(create_statement)
#         conn.commit()
#         log.logging_func('I',f'{create_statement}')
# except Exception as e:
#     log.logging_func('E',f'{e}')
# finally:
#     conn.close()

# try:
#     tables = query(sql.connect(f'fake_database_{datestring}.db'),'''
# SELECT 
#  *
#  FROM 
#  sqlite_schema
#  WHERE 
#  type ='table' AND 
#  name NOT LIKE 'sqlite_%'
# ''')

#     log.logging_func('I',f'PROFILES BELOW\n{tabulate(tables,headers ="keys", tablefmt="psql")}')

#     tables_list_A = tables['tbl_name'].to_list()
#     tables_list_B = tables['tbl_name'].to_list()
#     tables_list_C = tables['tbl_name'].to_list()
#     tables_list_D = tables['tbl_name'].to_list()
#     random.shuffle(tables_list_A)
#     random.shuffle(tables_list_B)
#     random.shuffle(tables_list_C)
#     random.shuffle(tables_list_D)
    
    
    
#     conn = sql.connect(f'fake_database_{datestring}.db')

#     cursor = conn.cursor()
#     row_num = 0
#     for a,b,c,d in zip(tables_list_A,tables_list_B,tables_list_C,tables_list_D):
#         row_num +=1
#         create_statement = f'''
    
# CREATE VIEW TEST_{''.join(set(random.choices(string.ascii_uppercase, k = S)))}_vw as
#     SELECT * FROM {a} A 
#     CROSS JOIN {b} B 
    
#     UNION ALL
    
#     SELECT * FROM {c} A 
#     CROSS JOIN {d} B 
    
# /*______[END VIEW DDL]______*/
# '''
#         cursor.execute(create_statement)
#         conn.commit()
#         log.logging_func('I',f'{create_statement}')

#     views = query(sql.connect(f'fake_database_{datestring}.db'),'''
#      SELECT 
#      *
#      FROM 
#      sqlite_schema
#      WHERE 
#      type ='view' AND 
#      name NOT LIKE 'sqlite_%'
#     ''')
    
#     log.logging_func('I',f'PROFILES BELOW\n{tabulate(views,headers ="keys", tablefmt="psql")}')

# except Exception as e:
#     log.logging_func('E',f'{e}')
##############################################################################################################
##############################################################################################################
#demo to test the view table parser, feeding a list of table objects, and parsing out the tables from the
# view DDL

views = pd.read_excel(fr'C:\Users\robba\OneDrive\Documents\BQ_InfoSchemaViews_20220222.xlsx')
tables = pd.read_excel(fr'C:\Users\robba\OneDrive\Documents\BQ_InfoSchemaTables_20220222 (1).xlsx')

views.reset_index(inplace=True)


try:
    tables_var = '|'.join(tables['table_catalog']+'.'+tables['table_schema']+'.'+tables['table_name'])
    test = view_to_table(views, objects=f'({tables_var})\W')
except Exception as e:
    log.logging_func('E',f'{e}')    
finally:
    total = test[['value', 'table_catalog',
           'table_schema', 'table_name', 'view_definition', 'check_option',
           'use_standard_sql']]
    total.reset_index(inplace=True)
    log.logging_func('I',f'PROFILES BELOW\n{tabulate(total,headers ="keys", tablefmt="psql")}')
    
total.to_excel('DataDictionary.xlsx', index=False)

quit()
#####################################################################################
test.columns

#####################################################################################
list_a = [1,1,2,2,3,3,4,4,5]
set_a=list(set(list_a))

