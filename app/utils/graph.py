import pandas as pd
import streamlit as st
import plotly.express as px

def get_brand_bar(brand):    
    fig = px.bar(brand, y='cnt', x='brand', text_auto=True, title="Brand")
    fig.update_xaxes(tickangle=40)
    return fig
    
def get_sold_pie(car):
    fig =  px.pie(car, values='cnt', names='names', title='Sold Car')
    return fig