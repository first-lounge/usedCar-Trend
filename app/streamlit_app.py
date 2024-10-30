import streamlit as st

def main():
    st.title("중고차 매매 데이터")

    with st.sidebar:
        st.sidebar.title("이것은 사이드바")
        st.sidebar.checkbox("확인")

if __name__ == "__main__":
    main()