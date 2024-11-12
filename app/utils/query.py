import streamlit as st
import pymysql # 전처리

def get_total_cnt(cursor):
    t1 = st.secrets["database"]["t1_name"]    
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

def get_daily_cnt(cursor):
    t2 = st.secrets["database"]["t2_name"]
    query = f"""
    SELECT COUNT(*)
    FROM `{t2}`
    WHERE crawled_at = CURDATE()
    """

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t2}`
    WHERE crawled_at = DATE(CURDATE() - INTERVAL 1 DAY)
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])

    return cnt

def get_weekly_cnt(cursor):
    t3 = st.secrets["database"]["t2_name"]
    query = f"""
    SELECT COUNT(*)
    FROM `{t3}`
    WHERE crawled_at = DATE(CURDATE() - INTERVAL 7 DAY)
    """

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t3}`
    WHERE crawled_at = DATE(DATE(CURDATE() - INTERVAL 1 DAY) - INTERVAL 7 DAY)
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])

    return cnt

def get_cnts():
    conn = pymysql.connect(host='localhost', user='root', passwd='!CLT-c403s', charset='utf8')
    cursor = conn.cursor()
    q1 = f"""USE car"""
    cursor.execute(q1)

    try:
        total = get_total_cnt(cursor)
        daily = get_daily_cnt(cursor)
        weekly = get_weekly_cnt(cursor)

        conn.commit()

        return total, daily, weekly
    
    finally:
        conn.close()