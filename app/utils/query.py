import streamlit as st
import pymysql 
import configparser   # ini 파일 읽기

# sql 연결
config = configparser.ConfigParser()
config.read('C:/Users/pirou/OneDrive/바탕 화면/중고차 매매 프로젝트/settings.ini')

def get_total_cnt(cursor, t1):  
    query = f"""SELECT COUNT(*) as cnt FROM `{t1}`"""

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t1}`
    WHERE crawled_at = CURDATE()
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])
    
    return cnt

def get_daily_cnt(cursor, t1):
    query = f"""
    SELECT COUNT(*)
    FROM `{t1}`
    WHERE 
        crawled_at = CURDATE()
        AND is_sold = 1
    """

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t1}`
    WHERE 
        crawled_at = DATE(CURDATE() - INTERVAL 1 DAY)
        AND is_sold = 1
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])

    return cnt

def get_weekly_cnt(cursor, t1):
    query = f"""
    SELECT COUNT(*)
    FROM `{t1}`
    WHERE 
        crawled_at = DATE(CURDATE() - INTERVAL 7 DAY)
        AND is_sold = 1
    """

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t1}`
    WHERE 
        crawled_at = DATE(DATE(CURDATE() - INTERVAL 1 DAY) - INTERVAL 7 DAY)
        AND is_sold = 1
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])

    return cnt

def get_cnts():
    conn = pymysql.connect(db=config['db_info']['db'], host=config['db_info']['host'], user=config['db_info']['user'], passwd=config['db_info']['passwd'], charset=config['db_info']['charset'])
    cursor = conn.cursor()

    # table
    t1 = st.secrets["database"]["t1_name"]
    try:
        total = get_total_cnt(cursor, t1)
        daily = get_daily_cnt(cursor, t1)
        weekly = get_weekly_cnt(cursor, t1)

        conn.commit()

        return total, daily, weekly
    
    finally:
        conn.close()