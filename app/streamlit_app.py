import streamlit as st
from utils.query import get_cnts, get_names
from utils.graph import get_brand_bar, get_sold_pie, get_not_sold_scatter

st.set_page_config(
page_title="중고차 매매 대시보드",
page_icon=":car:",
layout="wide",
initial_sidebar_state="expanded"
)

# 브랜드별 혹은 브랜드별 자동차별로 묶는 함수
def group_by_brand(tmp, brand_name):
    # 전체 브랜드별 개수
    if not brand_name:
        tmp['cnt'] = tmp.groupby(['brand'])['names'].transform('count')
        result = tmp[['brand', 'cnt']].drop_duplicates().sort_values(by='cnt', ascending=False)
    
    # 브랜드별 자동차별 개수
    else:
        tmp['cnt'] = tmp.groupby(['names'])['brand'].transform('count')
        result = tmp[tmp['brand'] == brand_name].drop_duplicates().sort_values(by='cnt', ascending=False)

    return result

def main():
    total, sold, daily, weekly = get_cnts()
    sold_car, not_sold = get_names()
    brand_names = not_sold['brand'].unique().tolist()

    # 사이드바
    with st.sidebar:
        st.sidebar.title(":mag: 브랜드 및 차량 선택")
        brand_name_selected = st.selectbox("Brand", brand_names, index=None, placeholder="Select brand name")

        if brand_name_selected:
            car_names = group_by_brand(not_sold, brand_name_selected)['names'].unique().tolist()
            car_name_selected = st.selectbox("Car", car_names, index=None, placeholder="Select car name")
        
    
    if not brand_name_selected:
        # 메인화면
        st.title(":oncoming_automobile: K Car 직영중고차 대시보드")
            
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("구매 가능 차량 수", f'{total[0]}', f'{total[1]}')
        m2.metric("전체 판매량", f'{sold}', f'{daily[0]}')
        m3.metric("일간", f'{daily[0]}', f'{daily[0] - daily[1]}')
        m4.metric("주간", f'{weekly[0]}', f'{weekly[0] - weekly[1]}')
        
        c1, c2 = st.columns([2,3])
        
        with c1:
            # brand 별 개수
            ranks = group_by_brand(sold_car, "")

            # 전체 브랜드
            st.subheader(':pushpin: Top 10 (팔린 차량)', divider="grey")
            st.dataframe(
                ranks.nlargest(10, 'cnt'),
                column_order=('brand', 'cnt'),
                hide_index=True,
                column_config={
                    'cnt': st.column_config.ProgressColumn(
                        label="Count",
                        format="%d",
                        
                        min_value=min(ranks['cnt']),
                        max_value=max(ranks['cnt'])
                    )
                }
            )
        with c2:
            ranks = group_by_brand(not_sold, "").nlargest(10, 'cnt')
            
            st.subheader(':bar_chart: Top 10 (구매 가능 차량)', divider='gray')
            st.plotly_chart(get_brand_bar(ranks))
        
        st.subheader(':blue_car: Not Sold List', divider='gray')
        st.dataframe(not_sold.iloc[:, :7], hide_index=True, column_config={'url': st.column_config.LinkColumn()})

    else:
        if not car_name_selected:
            # 선택한 브랜드별 메인화면
            st.title(f":oncoming_automobile: {brand_name_selected}")
            
            total = len(not_sold[not_sold['brand'] == brand_name_selected])
            sold = len(sold_car[sold_car['brand'] == brand_name_selected])
            avg_price = round(not_sold[not_sold['brand'] == brand_name_selected]['price'].mean())
            km = round(not_sold[not_sold['brand'] == brand_name_selected]['km'].mean())

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("구매 가능 차량 수", f'{total}')
            m2.metric("판매량", f'{sold}')
            m3.metric("평균 가격(단위: 만원)", f'{avg_price}')
            m4.metric("평균 KM", f'{format(km, ',d')}')

            c1, c2 = st.columns([2,3])
            
            # pie 차트
            with c1:
                ranks = group_by_brand(sold_car, brand_name_selected).nlargest(10, 'cnt')

                st.subheader(f':pushpin: Top 10', divider="grey")
                st.plotly_chart(get_sold_pie(ranks))

            with c2:
                # 선택한 브랜드 전체 차량 데이터
                st.subheader(f':pushpin: 구매 가능 차량', divider='gray')
                
                filtered_df = not_sold[not_sold['brand'] == brand_name_selected].iloc[:, 0:7]
                st.write(':moneybag: 가격(단위: 만원)')
                st.dataframe(data = filtered_df, hide_index=True, column_config={'url': st.column_config.LinkColumn()})

        else:
            # 선택한 브랜드별 차종별 메인화면
            st.title(f":oncoming_automobile: {car_name_selected}")
            
            total = len(not_sold[not_sold['names'] == car_name_selected])
            avg_price = round(not_sold[not_sold['names'] == car_name_selected]['price'].mean())
            km = round(not_sold[not_sold['names'] == car_name_selected]['km'].mean())

            m1, m2, m3 = st.columns(3)
            m1.metric("구매 가능 차량 수", f'{total}')
            m2.metric("평균 가격(단위: 만원)", f'{avg_price}')
            m3.metric("평균 KM", f'{format(km, ',d')}')
            
            c1, c2 = st.columns([2,3])
            filtered_df = not_sold[not_sold['names'] == car_name_selected].iloc[:, 0:7]

            with c1:
                st.subheader(f':chart_with_upwards_trend: 가격과 KM 분포  ', divider="grey")
                # scatter 차트
                st.plotly_chart(get_not_sold_scatter(filtered_df))
            with c2:
                # 선택한 브랜드의 특정 차종 데이터
                st.subheader(f':pushpin: 구매 가능 차량', divider='gray') 

                st.dataframe(data = filtered_df, hide_index=True, column_config={'url': st.column_config.LinkColumn()})

if __name__ == "__main__":

    main()