import streamlit as st
import pandas as pd

def main():
    st.title("중고차 매매 데이터")

    table_name = st.secrets["database"]["table_name"]
    with st.spinner("Loading..."):
        conn = st.connection("mysql", type="sql")


        query = f"""
        SELECT *
        FROM `{table_name}`
        """
        
        sold = pd.DataFrame(conn.query(query))
        sold.index += 1
    
    # id 컬럼의 콤마 제거
    st.dataframe(sold, column_config={"id": st.column_config.NumberColumn(format="%f")},)
    

    with st.sidebar:
        st.sidebar.title("이것은 사이드바")
        st.sidebar.checkbox("확인")

if __name__ == "__main__":
    main()