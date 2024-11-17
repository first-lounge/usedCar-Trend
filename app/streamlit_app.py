import pandas as pd
import streamlit as st
from utils.query import get_cnts, get_names

st.set_page_config(
page_title = "중고차 매매 대시보드",
layout="wide",
initial_sidebar_state="expanded"
)

conn = st.connection("mysql", type="sql")

def main():
    st.title("중고차 매매 데이터")

    total, daily, weekly = get_cnts()
    brand_names = get_names()['brand'].unique().tolist()
    car_names = get_names()['names'].unique().tolist()

    # 사이드바
    with st.sidebar:
        st.sidebar.title("중고차 매매 대시보드")
        brand_name_selected = st.multiselect("Select car name", ["All"] + brand_names, "All")
        car_name_selected = st.multiselect("Select car name", ["All"] + car_names, "All")

    
    c1, c3, c4 = st.columns(3)
    c1.metric("K Car 직영중고차", f'{total[0]}', f'{total[1]}')
    c3.metric("일간", f'{daily[0]}', f'{daily[0] - daily[1]}')
    c4.metric("주간", f'{weekly[0]}', f'{weekly[0] - weekly[1]}')

    st.divider()

    t1 = st.secrets["database"]["t1_name"]
    t2 = st.secrets["database"]["t2_name"]
    t3 = st.secrets["database"]["t3_name"]
    with st.spinner("Loading..."):
        query = f"""
        SELECT *
        FROM `{t2}`
        WHERE crawled_at = DATE(CURDATE() - INTERVAL 7 DAY)
        """
        
        weekly = pd.DataFrame(conn.query(query))
        weekly.index += 1
    
        day = f"""
        SELECT 
            `{t1}`.id, 
            `{t1}`.name, 
            `{t3}`.price,
            `{t3}`.pc_type, 
            `{t3}`.monthly_cost, 
            `{t1}`.model_year,
            `{t1}`.km,
            `{t1}`.fuel,
            `{t1}`.area,
            `{t1}`.url
        FROM `{t1}`
        JOIN `{t2}`
        ON `{t1}`.id = `{t2}`.id
        JOIN `{t3}`
        ON `{t1}`.id = `{t3}`.id
        WHERE 
            is_sold = 1
            AND sold_at = CURDATE()
        """
        
        daily = pd.DataFrame(conn.query(day))
        daily.index += 1

    # id 컬럼의 콤마 제거
    st.dataframe(weekly, column_config={"id": st.column_config.NumberColumn(format="%f")},)
    st.dataframe(daily, column_config={"id": st.column_config.NumberColumn(format="%f")},)

if __name__ == "__main__":

    main()