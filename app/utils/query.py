import pymysql 
import pandas as pd
import streamlit as st

# sql 연결
conn = pymysql.connect(db='car', host='0.tcp.jp.ngrok.io', user='root', passwd='!CLT-c403s', charset='utf8mb4', port=19366)
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

def get_sold_cnt():
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
    total = get_total_cnt()
    sold = get_sold_cnt()
    daily = get_daily_cnt()
    weekly = get_weekly_cnt()

    conn.commit()

    return total, sold, daily, weekly

def get_names():
    query = f"""
        SELECT *
        FROM `{t1}` m
        JOIN `{t2}` s
        ON m.id = s.id
        WHERE s.is_sold = 1
    """
    sold = pd.read_sql(query, conn)
    sold['brand'] = sold['name'].str.split().str[0]
    sold['names'] = sold['name'].str.split().str[1:5].str.join(' ')
    sold = sold[['brand', 'names']]

    query2 = f"""
        SELECT 
            DISTINCT m.name, 
            p.price,
            m.model_year,
            m.km,
            m.fuel,
            m.area,
            m.url
        FROM `{t1}` m
        JOIN `{t3}` p
        ON m.id = p.id
        JOIN `{t2}` s
        ON m.id = s.id
        WHERE s.is_sold = 0
    """
    all = pd.read_sql(query2, conn)
    all['brand'] = all['name'].str.split().str[0]
    all['names'] = all['name'].str.split().str[1:].str.join(' ')

    return sold, all