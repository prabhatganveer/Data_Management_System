import sys
# import re
import os
import time
from pathlib import Path

import pymysql
from datetime import date, datetime
import numpy as np
from colorama import init, Fore, Back, Style
import pandas as pd
from venv import logger

# todo ---------- get today date -----
datenow = datetime.now().strftime('%Y_%m_%d')
today = date.today()
init(autoreset=True)
Cyan = "\x1b[1;36;40m"
RED = '\033[1;36;31m'
GREEN = '\033[1;36;92m'


db_host = 'localhost'
db_user = 'root'
db_pass = 'xbyte'
db_name = f"pbs_master"
saerchterms_tb = f'searchterms_'

conn = pymysql.connect(host=db_host, user=db_user, password=db_pass, database=db_name, autocommit=True, use_unicode=True, charset="utf8")
cursor = conn.cursor()


try:
    # Ml_Path = r"D:\Prabhat_Ganveer\Training\masterlist\excel\20211229_Share_of_Search_Keyword.xlsx"
    Ml_Path = r"D:\Prabhat_Ganveer\Training\masterlist\excel\keywords.xlsx"
    df_dict = pd.read_excel(Ml_Path, sheet_name='For_All_12_SOS_Sites', usecols=['Keywords'])

    try:
        stmt = f"SHOW TABLES LIKE '{saerchterms_tb}'"
        cursor.execute(stmt)
        result = cursor.fetchone()
        if result:
            print(Fore.GREEN + "---------------Allready Exists------------"+ saerchterms_tb + Fore.RESET)
        else:
            try:
                #todo -------------------------------Create & Insert DataFrame to Table --------------------------------------

                tb_crea ='create table if not exists ' + saerchterms_tb + f""" (
                                                                  `Id` int(11) NOT NULL AUTO_INCREMENT,
                                                                  `Keywords` varchar(255) DEFAULT NULL,
                                                                  `Count` varchar(20) DEFAULT NULL,
                                                                  `Individual_Count` varchar(20) DEFAULT NULL,
                                                                  `Status` varchar(20) DEFAULT "Pending",
                                                                   PRIMARY KEY (`Id`)
                                                        )"""
                cursor.execute(tb_crea)
                print(Fore.GREEN + "---------------Table Created Successfully------------" + saerchterms_tb + Fore.RESET)
                no = 0
                for i, row in df_dict.iterrows():
                    no += 1
                    keyword = f"{row['Keywords']}"
                    if keyword:
                        try:
                            sql = f'INSERT INTO {saerchterms_tb} (`Keywords`) VALUES ("{keyword}")'
                            cursor.execute(sql)
                            conn.commit()
                        except Exception as e:
                            print('error in line--', e)
                    else:
                        print("keyword not find")
                print(Fore.GREEN + "---------------Insert Data in master_data_table------------"+ str(no) + Fore.RESET)
            except Exception as e:
                print('error in line--', e)
    except Exception as e:
        print('error in line--', e)
except Exception as E:
    print("Excel File path Wrong", E)
