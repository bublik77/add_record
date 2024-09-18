#!/usr/bin/env python3

import psycopg2
import os
from dotenv import load_dotenv
import csv

cvs_file = "data.csv"
path_cvs_file =  os.path.join(os.getcwd(), cvs_file)

try:
    load_dotenv()
except Exception as err:
    print(f"can't load env variables,{err}")

try:
    conn = psycopg2.connect(host=os.getenv('HOST'), port=os.getenv('PORT'), dbname=os.getenv('DB_NAME'), user=os.getenv('USER_NAME'), password=os.getenv('PASS'))
except:
    print("cant conect to db")

def list_db_records(args=0):
    fin_res = []
    if args == 0:
        cur = conn.cursor()
        cur.execute("SELECT * FROM company")
        records = cur.fetchall()
        for i in records:
            print (i)
        cur.close()
    else:
        #fin_res = []
        cur = conn.cursor()
        cur.execute("SELECT * FROM company where company_id = %s", (str(args)))
        res = cur.fetchall()
        for i in res:
            comp_id = str(i[0])
            comp_name = i[1]
            date_create = str(i[2])
            country = i[3]
            indastry = i[4]
            cout_pers = str(i[5])
            fin_res = [comp_id,comp_name,date_create,country,indastry,cout_pers]
        cur.close()
    #clean_res = [item.strip().strip("'") for item in fin_res]
    return fin_res

def get_id_from_db():
    cur = conn.cursor()
    cur.execute("SELECT company_id FROM company")
    records = cur.fetchall()
    list = []
    for i in records:
        list.append(i)
    cur.close()
    last_id =[x[0] for x in list]
    return last_id

def insert_data(file_in,id_in):
    with open(file_in, 'r', newline='') as file:
		reader = csv.reader(file)
        for i, row in enumerate(reader, 0):
            #print(type(row[0]))
            if int(row[0]) not in id_in:
                clean_row = [item.strip().strip("'") for item in row]
                cur = conn.cursor()
                cur.execute("INSERT INTO company (company_id, company_name, registration_date, country, industry, employee_count) VALUES (%s,%s,%s,%s,%s,%s)",(clean_row[0],clean_row[1],clean_row[2],clean_row[3],clean_row[4],clean_row[5]))
                conn.commit()
                print("New record added to the DB")
                print(clean_row)
                
            else:
                clean_row = [item.strip().strip("'") for item in row]
                #print(clean_row)
                #print(list_db_records(row[0]))
                if clean_row == list_db_records(row[0]):
                    print(f"data with id - {row[0]} exists in db")
                else:
                    print("different data")
                    print(f"data in file - {clean_row}")
                    print(f"data in db - {list_db_records(row[0])}")



if __name__== "__main__":
    #print(list_db_records(1))
    id_in_db = get_id_from_db()
    insert_data(path_cvs_file, id_in_db)
