import plotly.express as px
import plotly.graph_objects as go

def get_brand_bar(brand):    
    fig = px.bar(brand, y='cnt', x='brand', text_auto=True, color="brand", title="판매된 브랜드")
    fig.update_xaxes(tickangle=40)
    return fig
    
def get_sold_pie(car):
    fig =  px.pie(car, values='cnt', names='names', title='판매된 차량')
    return fig

def get_sold_scatter(sold, not_sold):
    # 그래프 생성
    fig = go.Figure()

    # sold 데이터셋
    fig.add_trace(go.Scatter(
        x=sold['price'],
        y=sold['km'],
        mode='markers',
        name='판매된 차량',
        marker=dict(size=10, color='#ff7f0e'),
        text='판매된 차량'
    ))

    # not_sold 데이터셋
    fig.add_trace(go.Scatter(
        x=not_sold['price'],
        y=not_sold['km'],
        mode='markers',
        name='구매 가능 차량',
        marker=dict(size=10, color='#17becf'),
        text='구매 가능 차량'
    ))

    # 레이아웃 설정
    fig.update_layout(title="가격별 주행거리 분포",
                    xaxis_title="가격",
                    yaxis_title="주행거리")

    return fig