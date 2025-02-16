import pymysql 
import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim

# 지도에 표시할 나라 설정
geo_local = Nominatim(user_agent='South Korea')

# sql 연결
conn = pymysql.connect(db=st.secrets['pymysql']['database'], host=st.secrets['pymysql']['host'], user=st.secrets['pymysql']['username'], passwd=st.secrets['pymysql']['password'], charset=st.secrets['pymysql']['charset'])
cursor = conn.cursor()

# table
t1 = st.secrets["db"]["t1_name"]
t2 = st.secrets["db"]["t2_name"]
t3 = st.secrets["db"]["t3_name"]

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
    FROM `{t1}` m
    join `{t2}` s
    on m.id = s.id
    WHERE
        s.is_sold = 1
        AND s.sold_at >= DATE(CURDATE() - INTERVAL 7 DAY)
        AND s.sold_at < CURDATE()
    """

    cursor.execute(query)
    cnt = [cursor.fetchall()[0][0]]

    query2 = f"""
    SELECT COUNT(*)
    FROM `{t1}` m
    join `{t2}` s
    on m.id = s.id
    WHERE
        s.is_sold = 1
        AND s.sold_at >= DATE(CURDATE() - INTERVAL 7 DAY)
        AND s.sold_at < DATE(CURDATE() - INTERVAL 1 DAY)
    """
    cursor.execute(query2)
    cnt.append(cursor.fetchall()[0][0])

    return cnt

def get_cnts():
    total = get_total_cnt()
    sold = get_sold_cnt()
    daily = get_daily_cnt()
    weekly = get_weekly_cnt()

    return total, sold, daily, weekly

def get_names():
    query = f"""
        SELECT 
            DISTINCT m.name, 
            p.price,
            m.model_year,
            m.km,
            m.fuel,
            m.area,
            m.url,
            s.is_sold
        FROM `{t1}` m
        JOIN `{t3}` p
        ON m.id = p.id
        JOIN `{t2}` s
        ON m.id = s.id
        WHERE s.is_sold = 1
    """
    sold = pd.read_sql(query, conn)
    sold['brand'] = sold['name'].str.split().str[0]
    sold['names'] = sold['name'].str.split().str[1:5].str.join(' ')

    query2 = f"""
        SELECT 
            DISTINCT m.name, 
            p.price,
            m.model_year,
            m.km,
            m.fuel,
            m.area,
            m.url,
            s.is_sold
        FROM `{t1}` m
        JOIN `{t3}` p
        ON m.id = p.id
        JOIN `{t2}` s
        ON m.id = s.id
        WHERE s.is_sold = 0
    """
    not_sold = pd.read_sql(query2, conn)
    not_sold['brand'] = not_sold['name'].str.split().str[0]
    not_sold['names'] = not_sold['name'].str.split().str[1:5].str.join(' ')

    return sold, not_sold

def geocoding(addr):
    try:
        geo = geo_local.geocode(addr)
        x_y = [geo.latitude, geo.longitude]
        return x_y
    except:
        return [0,0]

@st.cache_data
def get_map_datas():
    query = """
        select area, count(*)
        from main m
        join sales_list s
        on m.id = s.id
        where is_sold = 0
        group by area
    """

    cursor.execute(query)
    lists = cursor.fetchall()

    areas = {'서울':[0], '경기':[0], '인천':[0], '경남':[0],
                '경북':[0], '전남':[0], '전북':[0], '충남':[0], '충북':[0], '제주':[0], '강원':[0]}

    for item in lists:
        if item[0] == '전주':
            areas['전북'].append(item[0])
            areas['전북'][0] += item[1]

        elif item[0] == '청주':
            areas['충북'].append(item[0])
            areas['충북'][0] += item[1]
        
        elif item[0] == '원주':
            areas['강원'].append(item[0])
            areas['강원'][0] += item[1]

        elif item[0] == '제주':
            areas['제주'].append(item[0])
            areas['제주'][0] += item[1]

        elif item[0] in {'광주수완', '광주풍암'}:
            areas['전남'].append(item[0])
            areas['전남'][0] += item[1]
        
        elif item[0] in {'대전', '대전유성', '세종공주', '아산', '천안'}:
            areas['충남'].append(item[0])
            areas['충남'][0] += item[1]
        
        elif item[0] in {'경인', '인천'}:
            areas['인천'].append(item[0])
            areas['인천'][0] += item[1]

        elif item[0] in {'강남', '서초', '영등포', '장한평', '화곡'}:
            areas['서울'].append(item[0])
            areas['서울'][0] += item[1]

        elif item[0] in {'부산', '서부산', '양산', '울산', '창원마산', '해운대'}:
            areas['경남'].append(item[0])
            areas['경남'][0] += item[1]

        elif item[0] in {'구미', '대구', '대구반야월', '서대구', '포항'}:
            areas['경북'].append(item[0])
            areas['경북'][0] += item[1]

        else:
            areas['경기'].append(item[0])
            areas['경기'][0] += item[1]

    lat, lng, cnts = [], [], []
    names = list(areas.keys())

    for k in names:
        lat.append(geocoding(k)[0])
        lng.append(geocoding(k)[1])
        cnts.append(areas[k][0])
    
    return lat, lng, names, cnts