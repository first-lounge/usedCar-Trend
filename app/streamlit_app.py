import streamlit as st
import pandas as pd
from utils.query import get_cnt
def main():
    conn = st.connection("mysql", type="sql")
    
    total = get_cnt()
    st.title("중고차 매매 데이터")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("K Car 직영중고차", f'{total}', f'{12}')
    c2.metric("K Car 직영중고차 xxxx대", 15)
    c3.metric("일간", 15)
    c4.metric("주간", 15)

    st.divider()

    t2 = st.secrets["database"]["t2_name"]
    with st.spinner("Loading..."):
        query = f"""
        SELECT *
        FROM `{t2}`
        WHERE crawled_at = DATE(CURDATE() - INTERVAL 7 DAY)
        """
        
        weekly = pd.DataFrame(conn.query(query))
        weekly.index += 1
    
        day = f"""
        SELECT *
        FROM `{t2}`
        WHERE crawled_at = DATE(CURDATE() - INTERVAL 1 DAY)
        """
        
        daily = pd.DataFrame(conn.query(day))
        daily.index += 1

    # id 컬럼의 콤마 제거
    st.dataframe(weekly, column_config={"id": st.column_config.NumberColumn(format="%f")},)
    
    st.dataframe(daily, column_config={"id": st.column_config.NumberColumn(format="%f")},)

    with st.sidebar:
        st.sidebar.title("이것은 사이드바")
        st.sidebar.checkbox("확인")

if __name__ == "__main__":
    main()