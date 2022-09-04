"""
Created on Sat Mar 12 15:34:08 2022

@author: Zip Xanadu
"""

import threading
import time 
import random
import string
import json
from datetime import datetime as dt
import os
import pandas as pd
import numpy as np
import keyring 
from getpass import getuser
import requests
import io
import collections
from tabulate import tabulate
####################################
from Logging_Class import Logging
from PostgreSQL_Conn2 import PG
from Profiling_Class import Profiling
####################################
import warnings
try:
  
    postgres = PG(config={'username':'postgres',
                         'password':keyring.get_password('covered_db', 'postgres'),
                         'host':'localhost',
                         'database':'postgres',
                         'schema_name':'public'},active_logging=True)

    postgres.ddl_deploy('''
            DROP TABLE IF EXISTS public.chart_of_accounts CASCADE;
            CREATE TABLE public.chart_of_accounts(
            id bigserial primary key,
            account text,
            create_dt timestamp without time zone default now()
            );
            
        DROP TABLE IF EXISTS public.transactions CASCADE;               
        CREATE TABLE public.transactions(
            trans_id bigserial primary key,
            account bigint,
            trans_balance numeric default 0.00,
            confirmation text,
            insert_dt timestamp without time zone default now(),
            CONSTRAINT fk_accounts_1 FOREIGN KEY (account)
            REFERENCES public.chart_of_accounts (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            );
        
        DROP TABLE IF EXISTS public.current_balances CASCADE;   
        CREATE TABLE public.current_balances(
            account bigint primary key,
            current_balance numeric default 1000,
            last_confirmation text[],
            update_dt timestamp without time zone default now(),
            CONSTRAINT fk_accounts_2 FOREIGN KEY (account)
        REFERENCES public.chart_of_accounts (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION)''')
        
    postgres.ddl_deploy('''       
     create or replace function fn_cash_adj() returns trigger as $psql$
    declare 
    account bigint :=NEW.account; 
    current_balance numeric:= NEW.trans_balance;
    last_confirmation text[] :=ARRAY[NEW.confirmation];
    commit_time timestamp without time zone:= NEW.insert_dt;
    begin
         INSERT INTO public.current_balances(account,current_balance,last_confirmation,update_dt)
    	 VALUES(account,current_balance,last_confirmation,commit_time)
    	 ON CONFLICT ON CONSTRAINT current_balances_pkey DO UPDATE SET 
    	 current_balance = (EXCLUDED.current_balance+current_balances.current_balance)::numeric,
    	 update_dt = EXCLUDED.update_dt,
    	 last_confirmation = array_append(current_balances.last_confirmation,EXCLUDED.last_confirmation[1]::text);
    RETURN NEW;
    end;
    $psql$ 
    language plpgsql;
    create or replace trigger balance_adj 
    after insert or update on public.transactions for each row execute procedure fn_cash_adj()
        ''')
        
    postgres.ddl_deploy('''
    INSERT INTO public.chart_of_accounts (account) 
    SELECT LPAD(generate_series(1,5000)::text,6,'0')
    RETURNING *                    
                        ''')
    postgres.ddl_deploy('''
    INSERT INTO public.transactions (account,trans_balance,confirmation)
    SELECT id,15000,'INIT' FROM public.chart_of_accounts  
        ''')
        
except Exception as e:
    print(e)


def thread_runup(range_num,thread_num):
    try:
        postgres1 = PG(config={'username':'postgres',
                             'password':keyring.get_password('covered_db', 'postgres'),
                             'host':'localhost',
                             'database':'postgres',
                             'schema_name':'public'},active_logging=(False))
        for _ in range(range_num):
            try:
                value = round((random.randrange(1, 10000)-random.randrange(1, 10000))/3,2)
                account = random.randrange(1, 5000, 1)
                confirmation = ''.join(random.choice(string.ascii_letters) for x in range(5))
                postgres1.ddl_deploy(f'''/*THREAD:{thread_num}*/INSERT INTO public.transactions (account,trans_balance,confirmation) VALUES({account}, {value},'{confirmation}')''')
            except Exception as e:
                print(e)         
    except Exception as e:
        print(e)

def thread_up(threads):               
    thread = [None] * threads
    ###########################################################
    for i in range(threads):
        try:
            thread[i] = threading.Thread(target=thread_runup,name=(' THREAD {i}'), args=(10000,i,))
            thread[i].start() 
        except Exception as e:
            print(e)
                                                 
thread_up(50)
quit()


'''
Select A.account, sum(trans_balance), B.current_balance, count(A.*), sum(trans_balance)-B.current_balance as diff
from public.transactions A
inner join public.current_balances B on A.account = b.account
group by B.current_balance, A.account
--having ABS(sum(trans_balance)-B.current_balance) > 0.00
order by A.account
'''