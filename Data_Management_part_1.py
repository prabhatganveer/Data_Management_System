import sys
# import re
import os
import time
from pathlib import Path
import pandas as pd
import pymysql
from datetime import date, datetime
import numpy as np

from colorama import init, Fore, Back, Style

# todo ---------- get today date -----
datenow = datetime.now().strftime('%Y_%m_%d')
today = date.today()
init(autoreset=True)
Cyan = "\x1b[1;36;40m"
RED = '\033[1;36;31m'
GREEN = '\033[1;36;92m'
Purple='\033[0;35m'

print()

def read_xlsx():
    try:
        print(f"\n{Cyan}Please Enter Master List Excel Full Path :", end=' ')
        Ml_Path = input()

        df_dict = pd.read_excel(Ml_Path,sheet_name='Category Keyword',usecols=['Market','eCustomer', 'Category','Brand','Keywords'], nrows=0)

        insert_column = str(tuple(df_dict.columns)).replace("'", '`')
        # query = "`Id` int(10) NOT NULL AUTO_INCREMENT,\n"
        # for col_name in df_dict.columns:
        #     query += f"`{col_name}` mediumtext,\n"

        len_values = len(tuple(df_dict.columns))
        values = []

        for i in range(len_values):
            values.append("%s")
        values = str(tuple(values)).replace("'", '')
        print(f"\n{Cyan}------------- >> Reading Excel File..........")
        data = pd.read_excel(Ml_Path,sheet_name='Category Keyword',usecols=['Market','eCustomer', 'Category','Brand','Keywords'])

        return data, insert_column, values, len_values

    except Exception as E:
        print("Excel File path Wrong", E)
        read_xlsx()

# todo -- Call -- read_xlsx() functions ------
data, insert_column_names, values, len_values = read_xlsx()

df = pd.DataFrame(data)
index = df.index
number_of_rows = len(index)

print(f'{Cyan} ------------- >> Reading Excel File is Completed ..........')


db_host = 'localhost' #int(input())
db_user = 'root'
db_pass = 'xbyte'

# print(f"\n{Cyan}Please Enter Database Name : ", end='')
# db_name = input()
db_name = "p&g_master_config"
db_con = pymysql.connect(host=db_host, user=db_user, password=db_pass)
db_cursor = db_con.cursor()
# create_db = f"create database if not exists {db_name} CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci"
# db_cursor.execute(create_db)

con = pymysql.connect(host=db_host, user=db_user, password=db_pass, database=db_name,  autocommit=True,use_unicode=True, charset="utf8")
cursor = con.cursor()
print(Fore.GREEN+"---------------Database Created Successfully------------"+Fore.RESET)

table_name = 'brand_cat_keyword_master'
backup_table = f'{table_name}_{datenow}'
try:
    # todo  --------- Create Table -----------------

    stmt = f"SHOW TABLES LIKE '{table_name}'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    if result:
        try:
            backup_tb = f'CREATE TABLE {backup_table} AS SELECT * FROM {table_name}'
            # print(backup_tb)
            cursor.execute(backup_tb)
            truncat = f"TRUNCATE TABLE {table_name}"
            cursor.execute(truncat)
        except:
            pass

        count = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(count)
        res = cursor.fetchall()
        for i in res:
            count1 = f'{i[0]}'

        print(count1)
        # time.sleep(5)
        if int(count1) != 0:
            print("Backup complated")
        else:
            import datetime
            start_time = datetime.datetime.now()
            # print(f"\n{Cyan} Start time:" + str(start_time))
            sql = f'INSERT INTO {table_name} {insert_column_names} VALUES {values}'
            print(sql)

            param2 = []
            df = df.replace(np.nan," ")
            for data in df.itertuples():
                param1 = []
                for i in range(len_values):
                    if data:
                        param1.append(str(data[i + 1]))
                    else:
                        param1.append(str(data[0]))
                param2.append(param1)
            cursor.executemany(sql, param2)
            con.commit()
            try:
                updt = 'UPDATE `brand_cat_keyword_master` SET `brand1` = `brand`'
                cursor.execute(updt)
                con.commit()

                updt = 'UPDATE `brand_cat_keyword_master` SET `brand`=REPLACE(brand,"&","")'
                cursor.execute(updt)
                con.commit()
                updt = 'UPDATE `brand_cat_keyword_master` SET `brand`=REPLACE(brand,"-","")'
                cursor.execute(updt)
                con.commit()
                updt = 'UPDATE `brand_cat_keyword_master` SET brand=REPLACE(brand," ","")'
                cursor.execute(updt)
                con.commit()
            except:
                pass
            print(Fore.GREEN + f"<---------- Successfully inserted data in Input table ------------>" + Fore.RESET)

            end_time = datetime.datetime.now()
            # print(f"\n{Cyan} End time :" + str(end_time))

            diff_time = end_time - start_time
            print(f"\n{Cyan} Time Durations of Inserting data.......:" + str(diff_time))
            print()
    else:
        # todo -------------------- Read query from Input Text file ------------------------
        try:

        # todo ---------- Insert DataFrame to Table -------------

            import datetime
            start_time = datetime.datetime.now()
            # print(f"\n{Cyan} Start time:" + str(start_time))


            tb_crea ='create table if not exists ' + table_name + f""" (  
                                                              `Id` int(11) NOT NULL AUTO_INCREMENT,
                                                              `Market` varchar(50) DEFAULT NULL,
                                                              `eCustomer` varchar(50) DEFAULT NULL,
                                                              `Category` varchar(50) DEFAULT NULL,
                                                              `Brand1` varchar(50) DEFAULT NULL,
                                                              `Keywords` varchar(50) DEFAULT NULL,
                                                              `Brand` varchar(50) DEFAULT NULL,
                                                               PRIMARY KEY (`Id`)
                                                    )"""
            # print(tb_crea)
            cursor.execute(tb_crea)

            sql = f'INSERT INTO {table_name} {insert_column_names} VALUES {values}'
            print(sql)
            param2 = []
            df = df.replace(np.nan,"")
            for data in df.itertuples():

                param1 = []
                for i in range(len_values):
                    if data:
                        param1.append(str(data[i + 1]))
                    else:
                        param1.append(str(data[0]))
                param2.append(param1)

            print(sql,param2)
            # breakpoint()
            cursor.executemany(sql, param2)
            con.commit()
            try:
                updt = 'UPDATE `brand_cat_keyword_master` SET `brand1` = `brand`'
                cursor.execute(updt)
                con.commit()

                updt = 'UPDATE `brand_cat_keyword_master` SET `brand`=REPLACE(brand,"&","")'
                cursor.execute(updt)
                con.commit()

                updt = 'UPDATE `brand_cat_keyword_master` SET `brand`=REPLACE(brand,"-","")'
                cursor.execute(updt)
                con.commit()

                updt = 'UPDATE `brand_cat_keyword_master` SET `brand`=REPLACE(brand," ","")'
                cursor.execute(updt)
                con.commit()

            except:
                pass
            print(Fore.GREEN + f"<---------- Successfully inserted data in Input table ------------>" + Fore.RESET)

            end_time = datetime.datetime.now()
            # print(f"\n{Cyan} End time :" + str(end_time))

            diff_time = end_time - start_time
            print(f"\n{Cyan} Time Durations of Inserting data.......:" + str(diff_time))
            print()

        except Exception as e:
            print(Fore.RED + "Error in Input Query :" + Fore.RESET, e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)


except Exception as e:
    print(Fore.RED+"<---- Invalid Input table Name Argument ---->"+Fore.RESET,e)
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
time.sleep(10)
