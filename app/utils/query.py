import pymysql 
import configparser   # ini 파일 읽기
import pandas as pd
import streamlit as st

# sql 연결
config = configparser.ConfigParser()
config.read('C:/Users/pirou/OneDrive/바탕 화면/중고차 매매 프로젝트/settings.ini')
conn = pymysql.connect(db=config['db_info']['db'], host=config['db_info']['host'], user=config['db_info']['user'], passwd=config['db_info']['passwd'], charset=config['db_info']['charset'])
cursor = conn.cursor()

# table
t1 = st.secrets["database"]["t1_name"]
t2 = st.secrets["database"]["t2_name"]
t3 = st.secrets["database"]["t3_name"]

def get_total_cnt():  
    query = f"""SELECT COUNT(*) FROM `{t1}`"""

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t2}`
    WHERE crawled_at = CURDATE()
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])
    
    return cnt

def get_daily_cnt():
    query = f"""
    SELECT COUNT(*)
    FROM `{t2}`
    WHERE 
        is_sold = 1
        AND sold_at = CURDATE()
    """

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t2}`
    WHERE 
        is_sold = 1
        AND sold_at = DATE(CURDATE() - INTERVAL 1 DAY)
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])

    return cnt

def get_weekly_cnt():
    query = f"""
    SELECT COUNT(*)
    FROM `{t2}`
    WHERE
        is_sold = 1
        AND sold_at = DATE(CURDATE() - INTERVAL 7 DAY)
        
    """

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t2}`
    WHERE 
        is_sold = 1
        AND sold_at = DATE(DATE(CURDATE() - INTERVAL 1 DAY) - INTERVAL 7 DAY)
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])

    return cnt

def get_cnts():
    # try:
    total = get_total_cnt()
    daily = get_daily_cnt()
    weekly = get_weekly_cnt()
    conn.commit()

    return total, daily, weekly
    
    # finally:
    #     conn.close()

def get_names():
    query = """
    SELECT name
    FROM main
    """
    result = pd.read_sql(query, conn)
    result['brand'] = result['name'].str.split().str[0]
    result['names'] = result['name'].str.split().str[1:3].str.join(' ')
    result.drop('name', axis=1, inplace=True)

    return result