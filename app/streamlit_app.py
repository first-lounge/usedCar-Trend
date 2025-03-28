import folium
import pandas as pd
import streamlit as st
from utils.query import get_cnts, get_names, get_map_datas
from utils.graph import get_brand_bar, get_sold_pie, get_sold_scatter

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
    
    # 브랜드별 자동차별 개수
    else:
        tmp['cnt'] = tmp.groupby(['names'])['brand'].transform('count')

    return tmp

def main():
    total, sold, daily, weekly = get_cnts() # 총 대수, 총 판매량, 일간 판매량, 주간 판매량
    sold_car, not_sold_car = get_names()    # 판매된 차량, 구매 가능 차량
    brand_names = sold_car['brand'].unique().tolist()

    # 사이드바
    with st.sidebar:
        st.sidebar.title(":mag: 브랜드 및 차량 선택")
        brand_name_selected = st.selectbox("Brand", brand_names, index=None, placeholder="Select brand name")

        if brand_name_selected:
            car_names = sold_car[sold_car['brand'] == brand_name_selected]['names'].unique().tolist()
            car_name_selected = st.selectbox("Car", car_names, index=None, placeholder="Select car name")
            
    if not brand_name_selected:
        # 메인화면
        st.title(":oncoming_automobile: K Car 직영중고차 대시보드")
            
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("구매 가능 차량 수", f'{total[0]}', f'{total[1]}')
        m2.metric("전체 판매량", f'{sold}', f'{daily[0]}')
        m3.metric("일간", f'{daily[0]}', f'{daily[0] - daily[1]}')
        m4.metric("주간", f'{weekly[0]}', f'{weekly[0] - weekly[1]}')
        
        c1, c2 = st.columns([3,3])
        filtered = group_by_brand(sold_car, "")
        
        # brand별 판매 개수
        with c1:
            st.subheader(':bar_chart: Top 10', divider='gray')
            st.plotly_chart(get_brand_bar(filtered[['brand', 'cnt']].drop_duplicates().nlargest(10, 'cnt')))

        with c2:
            st.subheader(':world_map: Map(구매 가능 차량)', divider='gray')

            lat, lng, names, cnts = get_map_datas() # 위도, 경도, 지역 이름, 차량 대수

            # 지도 데이터
            map_data = pd.DataFrame({
                'lat': lat,
                'lng': lng,
                'name' : names,
                'cnt' : cnts
            })
            
            # 지도 객체 생성
            my_map = folium.Map(
                location=[map_data['lat'].mean()-1, map_data['lng'].mean()], 
                zoom_start=6)
                
            # 지도 커스텀
            # 지도에 원형 마커와 값 추가
            for index, row in map_data.iterrows():       # 데이터프레임 한 행 씩 처리

                folium.CircleMarker(                     # 원 표시
                    location=[row['lat'], row['lng']],   # 원 중심- 위도, 경도
                    radius=9,             # 원의 반지름
                    color='orange',                        # 원의 테두리 색상
                    fill=True,                           # 원을 채움
                    fill_opacity=1.0                     # 원의 내부를 채울 때의 투명도
                ).add_to(my_map)                         # my_map에 원형 마커 추가

                folium.Marker(                           # 값 표시
                    location=[row['lat'], row['lng']],   # 값 표시 위치- 위도, 경도
                    tooltip=row['name'],
                    icon=folium.DivIcon(
                        html=f"<div>{row['cnt']}</div>"), # 값 표시 방식
                ).add_to(my_map)                         # my_map에 값 추가

            # 지도 시각화
            st.components.v1.html(my_map._repr_html_(), width=700, height=600)
        
        st.subheader(':blue_car: 구매 가능 차량 리스트', divider='gray')
        st.write(':moneybag: price(단위: 만원)')
        st.dataframe(not_sold_car.iloc[:, :7], hide_index=True, column_config={'url': st.column_config.LinkColumn()})

    else:
        filtered = group_by_brand(sold_car, brand_name_selected)
        
        # 선택한 브랜드 시 나오는 화면
        if not car_name_selected:
            st.title(f":oncoming_automobile: {brand_name_selected}")
            
            sold = len(sold_car[sold_car['brand'] == brand_name_selected])
            total = len(not_sold_car[not_sold_car['brand'] == brand_name_selected])
            avg_price = round(not_sold_car[not_sold_car['brand'] == brand_name_selected]['price'].mean())
            km = round(not_sold_car[not_sold_car['brand'] == brand_name_selected]['km'].mean())

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("판매량", f'{sold}')
            m2.metric("구매 가능 차량 수", f'{total}')
            m3.metric("평균 가격(단위: 만원)", f'{avg_price}')
            m4.metric("평균 KM", f'{format(km, ',d')}')

            c1, c2 = st.columns([2,3])
            
            # pie 차트
            with c1:
                st.subheader(f':pushpin: Top 10', divider="grey")
                st.plotly_chart(get_sold_pie(filtered[filtered['brand'] == brand_name_selected][['names', 'cnt']].drop_duplicates().nlargest(10, 'cnt')))

            with c2:
                # 선택한 브랜드 전체 차량 데이터
                st.subheader(f':pushpin: 구매 가능 차량', divider='gray')
                
                filtered_df = not_sold_car[not_sold_car['brand'] == brand_name_selected].iloc[:, 0:7]
                st.write(':moneybag: price(단위: 만원)')
                st.dataframe(data = filtered_df, hide_index=True, column_config={'url': st.column_config.LinkColumn()})

        # 선택한 브랜드별 차종별 메인화면
        else:
            st.title(f":oncoming_automobile: {car_name_selected}")
            sold = len(sold_car[sold_car['names'] == car_name_selected])
            not_sold = len(not_sold_car[not_sold_car['names'] == car_name_selected])
            avg_price = round(sold_car[sold_car['names'] == car_name_selected]['price'].mean())
            km = round(sold_car[sold_car['names'] == car_name_selected]['km'].mean())

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("구매 가능 차량 수", f'{sold}')
            m2.metric("구매 가능 차량 수", f'{not_sold}')
            m3.metric("평균 가격(단위: 만원)", f'{avg_price}')
            m4.metric("평균 KM", f'{format(km, ',d')}')
            
            # scatter 차트
            st.subheader(f':chart_with_upwards_trend: 주행거리별 가격 분포', divider="grey")
            
            scatter_sold_car = filtered[filtered['brand'] == brand_name_selected][['names', 'price', 'km','is_sold', 'cnt']].drop_duplicates()
            
            not_filtered = group_by_brand(not_sold_car, brand_name_selected)
            scatter_not_sold_car = not_filtered[not_filtered['brand'] == brand_name_selected][['names', 'price', 'km', 'is_sold', 'cnt']].drop_duplicates()

            
            st.plotly_chart(get_sold_scatter(scatter_sold_car[scatter_sold_car['names'] == car_name_selected], scatter_not_sold_car[scatter_not_sold_car['names'] == car_name_selected]))
            
            # 브랜드별 차량별 데이터 중 구매 가능 차량
            st.subheader(f':pushpin: 구매 가능 차량', divider='gray') 
            st.dataframe(data = not_sold_car[not_sold_car['names'] == car_name_selected].iloc[:, 0:7], hide_index=True, column_config={'url': st.column_config.LinkColumn()})

if __name__ == "__main__":

    main()