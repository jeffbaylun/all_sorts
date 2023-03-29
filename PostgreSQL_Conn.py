# -*- coding: utf-8 -*-
"""
Created on Sat Feb 26 15:00:19 2022

@author: Nicholas Krauss


"""
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from Logging_Class import Logging
import keyring
from getpass import getuser
import os
from datetime import datetime as dt
import time
import io

class PG(object):
    '''
    1.)instantiate your class with a dict containing the following key value pairs
        -username
        -password
        -host
        -database
        -schema_name
    
    2.)indicate if you wish to include active_logging or just have messages printed to the screen
    
    '''
    def __init__(self, config, active_logging=False): 
        try:
            config_blank = {'username':'','password':'','host':'','database':'','schema_name':''}
            key1= set(config_blank.keys())
            for key, value in config.items():
                setattr(self, key, value)
            assert key1 == set(config.keys()), f'REQUIRE: {key1}\nRECEIVED: {set(config.keys())}'

            self.active_logging = active_logging
            if self.active_logging == True:
                self.log = self.logging()
                
            self.connection = self.pg_conn()
        except Exception as e:
            print(e)
    
    def logging(self):
        try:
            datestring = dt.strftime(dt.now(),'%Y%m%d_%H%M%S')
            log = Logging(task=f'{self.host}_{datestring}', 
                          filename=f'{self.host}_{datestring}.log',log_print=True)
            log.logging_folder()
        except Exception as e:
            print(e)
        return log
    
    def pg_conn(self):
        '''
        This is instantiated with the class above, all dict values are passed to and create the connection
        '''
        try:
            engine = create_engine(f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:5432/{self.database}?options=-csearch_path%3D{self.schema_name}')
            conn = engine.raw_connection()
            cursor = conn.cursor()
            if self.active_logging == True:
                self.log.logging_func('I', f'CONNECTED TO {self.host}')
            else:   
                print(f'CONNECTED TO {self.host}')
        except Exception as e:
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)
                
        return [conn,cursor,engine]

    def query(self, sql_syntax):
        '''
        Simply pass correct and complete sql syntax and receive a dataframe
        
        Ex. 'Select to_char(now()::date,'YYYYMMDD')::numeric::money'
                        -TIME EQUALS MONEY
        '''
        try:
            t = time.perf_counter()
            data = pd.read_sql(sql_syntax,self.connection[0])
            elapsed = round((time.perf_counter()-t)/60,2)
            print(f'{len(data)},{elapsed}')
            if self.active_logging == True:
                self.log.logging_func('I', f'\n***SQL SYNTAX START***\n\t{sql_syntax}\n***SQL SYNTAX END***\n[ROW COUNT]:{len(data)},[ELAPSED]:{elapsed}')   
            else:   
                print(f'\n***SQL SYNTAX START***\n\t{sql_syntax}\n***SQL SYNTAX END***\n[ROW COUNT]:{len(data)},[ELAPSED]:{elapsed}')  
        except Exception as e:        
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)
        return data   

    def full_upload_statement(self, df, table_name, replace_or_append):
        '''
        Pass the following
        1.) 'dataframe'
        2.) 'table_name' NOTE: this is going to create table in the schema of the connection_object
        3.) indicate either 'replace' or 'append' if the table does not exist and you select 'append'
            it will create new.
        '''
        try:
            df.head(0).to_sql(table_name, self.connection[2], if_exists= replace_or_append, index=False)    
            conn = self.connection[0]
            cur =  self.connection[1]
            output = io.StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            cur.copy_from(output, table_name, null="")
            conn.commit()
            if self.active_logging == True:
                self.log.logging_func('I', f'SUCCESSFULLY LOADED {table_name} ROWS TOTAL')
            else:   
                print(f'SUCCESSFULLY INSERTED {table_name} ROWS TOTAL')  
        except Exception as e:
            cur.execute('''ROLLBACK''')
            cur.close()
            conn.commit()
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)               

    def insert_statement(self,insert_query,df):
        '''
        For a more nuanced loading and to take advantage of column defaults and
        incrementing serial columns
        
        Pass the following:
            1.) the insert query 
            2.) the dataframe containing the columns to be loaded
        
        '''
        try:
            cur = self.connection[1]
            load = list(df.itertuples(index=False, name=None))
            dynamic_insert = ','.join(['%s']*len(load))
            insert_query_format = f'''{insert_query}'''.format(dynamic_insert)
            cur.execute(insert_query_format,load)
            self.connection[0].commit()

            if self.active_logging == True:
                self.log.logging_func('I', f'\n***SQL SYNTAX START***\n\t{insert_query}\n***SQL SYNTAX END***\nSUCCESSFULLY INSERTED {len(df)} ROWS TOTAL')
            else:   
                print(f'\n***SQL SYNTAX START***\n\t{insert_query}\n***SQL SYNTAX END***\nSUCCESSFULLY INSERTED {len(df)} ROWS TOTAL')    
       
        except Exception as e:        
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)

    def ddl_deploy(self,sql):
        '''
        To run DDL and create new objects in PostgreSQL
        NOTE: you can create objects outside of your principal connection
        schema 
        '''
        try:
            cur = self.connection[1]
            cur.execute(sql)
            self.connection[0].commit()
    
            if self.active_logging == True:
                self.log.logging_func('I', f'{sql} DEPLOYED')
            else:   
                print(f'{sql} DEPLOYED')           
        except Exception as e:
            cur.execute('''ROLLBACK''')
            self.connection[0].commit()
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)             
   
          
   