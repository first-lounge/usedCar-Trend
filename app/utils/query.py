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
    query = f"""
    SELECT COUNT(*) 
    FROM `{t1}`
    JOIN `{t2}`
    ON `{t1}`.id = `{t2}`.id
    WHERE `{t2}`.is_sold = 0
    """

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

def get_total_sold():
    query = f"""SELECT COUNT(*) FROM `{t2}` WHERE is_sold = 1"""
    cursor.execute(query)
    cnt = cursor.fetchall()[0][0]

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
    sold = get_total_sold()
    daily = get_daily_cnt()
    weekly = get_weekly_cnt()
    conn.commit()

    return total, sold, daily, weekly

def get_names():
    query = """
        SELECT *
        FROM main m
        JOIN sales_list s
        ON m.id = s.id
        WHERE s.is_sold = 1
    """
    tmp = pd.read_sql(query, conn)
    tmp['brand'] = tmp['name'].str.split().str[0]
    tmp['names'] = tmp['name'].str.split().str[1:5].str.join(' ')
    sold = tmp[['brand', 'names']]

    query2 = """
        SELECT 
            DISTINCT m.name, 
            p.price,
            m.model_year,
            m.km,
            m.fuel,
            m.area,
            m.url
        FROM main m
        JOIN price_info p
        ON m.id = p.id
    """
    tmp = pd.read_sql(query2, conn)
    tmp['brand'] = tmp['name'].str.split().str[0]
    tmp['names'] = tmp['name'].str.split().str[1:].str.join(' ')

    return sold, tmp