import psycopg2
import pandas as pd
import streamlit as st

# 함수로 DB 연결
def get_connection():
    conn = psycopg2.connect(
        dbname=st.secrets['psql']['database'],
        host=st.secrets['psql']['host'],
        user=st.secrets['psql']['username'],
        password=st.secrets['psql']['password']
    )
    conn.autocommit=True  # 최신 데이터 바로 조회 가능
    return conn

# table
t1 = st.secrets["db"]["t1_name"]
t2 = st.secrets["db"]["t2_name"]
t3 = st.secrets["db"]["t3_name"]

def get_total_cnt():  
    result = {}
    query = f"""
    SELECT COUNT(*) 
    FROM "{t1}" 
    JOIN "{t2}" 
    ON "{t1}".id = "{t2}".id 
    WHERE "{t2}".is_sold = FALSE
    """
    query2 = f""" SELECT COUNT(*) FROM "{t2}" WHERE crawled_at = CURRENT_DATE"""
    
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        result["total"] = cursor.fetchone()[0]

        cursor.execute(query2)
        result["today"] = cursor.fetchone()[0]

    conn.close()
    return result

def get_sold_cnt():
    query = f"""SELECT COUNT(*) FROM "{t2}" WHERE is_sold = TRUE"""
    
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        cnt = cursor.fetchone()[0]
    
    conn.close()
    return cnt

def get_daily_cnt():
    result = {}

    query = f"""
    SELECT COUNT(*)
    FROM "{t2}"
    WHERE 
        is_sold = TRUE
        AND sold_at = CURRENT_DATE
    """
    query2 = f"""
    SELECT COUNT(*)
    FROM "{t2}"
    WHERE is_sold = TRUE
        AND sold_at = CURRENT_DATE - INTERVAL '1 day'
    """
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        result["today"] = cursor.fetchone()[0]

        cursor.execute(query2)
        result["yesterday"] = cursor.fetchone()[0]

    conn.close()
    return result

def get_weekly_cnt():
    result = {}

    # 이번 주 (월요일 시작)
    query = f"""
    SELECT
        DATE_TRUNC('week', (NOW() AT TIME ZONE 'Asia/Seoul'))::DATE AS week_start,
        LPAD(TO_CHAR(DATE_TRUNC('week', CURRENT_DATE), 'WW'), 2, '0') AS week_num,
        COUNT(m.id) as cnt
    FROM "{t1}" m
    LEFT JOIN "{t2}" s
    ON m.id = s.id
    WHERE
        s.is_sold = TRUE
        AND DATE_TRUNC('week', s.sold_at) = DATE_TRUNC('week', CURRENT_DATE)
    GROUP BY
        week_start,
        week_num
    """
    
    # 저번 주
    query2 = f"""
    SELECT
        COUNT(m.id) AS cnt
    FROM
        "{t1}" m
    LEFT JOIN
        "{t2}" s ON m.id = s.id
    WHERE
        s.is_sold = TRUE
        AND DATE_TRUNC('week', s.sold_at) = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 week')
    """

    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        tmp = cursor.fetchone()
        if tmp:
            result["week_start"], result["week_num"], result["this_week"] = tmp
        else:
            result["week_start"], result["week_num"], result["this_week"] = None, None, None

        cursor.execute(query2)
        result["last_week"] = cursor.fetchone()[0]

    conn.close()
    return result

def get_cnts():
    total = get_total_cnt()
    sold = get_sold_cnt()
    daily = get_daily_cnt()
    weekly = get_weekly_cnt()

    return total, sold, daily, weekly

def get_names():
    conn = get_connection()

    query_sold = f"""
        SELECT 
            DISTINCT m.name, 
            p.price,
            m.model_year,
            m.km,
            m.fuel,
            m.area,
            m.url,
            s.is_sold
        FROM "{t1}" m
        JOIN "{t3}" p
        ON m.id = p.id
        JOIN "{t2}" s
        ON m.id = s.id
        WHERE s.is_sold = TRUE
    """
    query_not_sold = f"""
        SELECT 
            DISTINCT m.name, 
            p.price,
            m.model_year,
            m.km,
            m.fuel,
            m.area,
            m.url,
            s.is_sold
        FROM "{t1}" m
        JOIN "{t3}" p
        ON m.id = p.id
        JOIN "{t2}" s
        ON m.id = s.id
        WHERE s.is_sold = FALSE
    """

    sold = pd.read_sql(query_sold, conn)
    sold['brand'] = sold['name'].str.split().str[0]
    sold['names'] = sold['name'].str.split().str[1:5].str.join(' ')


    not_sold = pd.read_sql(query_not_sold, conn)
    not_sold['brand'] = not_sold['name'].str.split().str[0]
    not_sold['names'] = not_sold['name'].str.split().str[1:5].str.join(' ')

    conn.close()
    return sold, not_sold

@st.cache_data(ttl=4*60*60)
def get_map_datas():
    conn = get_connection()
    
    query = f"""
        SELECT 
            area, 
            COUNT(*)
        FROM "{t1}" m
        JOIN "{t2}" s
        ON m.id = s.id
        WHERE s.is_sold = FALSE
        GROUP BY area
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        lists = cursor.fetchall()
    conn.close()

    areas = {'서울':0, '경기':0, '인천':0, '경남':0,
                '경북':0, '전남':0, '전북':0, '충남':0, '충북':0, '제주':0, '강원':0}
    
    area_coords = {
    '서울': [37.5665, 126.9780],   # 서울특별시청
    '경기': [37.2753, 127.0090],   # 경기도청
    '인천': [37.4563, 126.7052],   # 인천광역시청
    '경남': [35.1968, 128.0355],   # 경상남도청
    '경북': [36.5762, 128.6230],   # 경상북도청
    '전남': [34.8161, 126.4627],   # 전라남도청
    '전북': [35.8206, 127.1086],   # 전라북도청
    '충남': [36.6580, 126.6720],   # 충청남도청
    '충북': [36.6353, 127.4910],   # 충청북도청
    '제주': [33.5006, 126.5312],   # 제주특별자치도청
    '강원': [37.8813, 127.7298]    # 강원도청
    }
    
    for item in lists:
        if item[0] == '전주':
            areas['전북'] += item[1]

        elif item[0] == '청주':
            areas['충북'] += item[1]
        
        elif item[0] == '원주':
            areas['강원'] += item[1]

        elif item[0] == '제주':
            areas['제주'] += item[1]

        elif item[0] in {'광주수완', '광주풍암'}:
            areas['전남'] += item[1]
        
        elif item[0] in {'대전', '대전유성', '세종공주', '아산', '천안'}:
            areas['충남'] += item[1]
        
        elif item[0] in {'경인', '인천'}:
            areas['인천'] += item[1]

        elif item[0] in {'강남', '서초', '영등포', '장한평', '화곡'}:
            areas['서울'] += item[1]

        elif item[0] in {'부산', '서부산', '양산', '울산', '창원마산', '해운대'}:
            areas['경남'] += item[1]

        elif item[0] in {'구미', '대구', '대구반야월', '서대구', '포항'}:
            areas['경북'] += item[1]

        else:
            areas['경기'] += item[1]

    lat, lng, cnts = [], [], []

    for key, val in area_coords.items():
        lat.append(val[0])
        lng.append(val[1])
        cnts.append(areas[key])
    
    return lat, lng, list(area_coords.keys()), cnts