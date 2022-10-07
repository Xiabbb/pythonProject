from datetime import datetime, time

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

st.write('Hello world!')

st.header('st.button')  # 创建标头文本

if st.button('Say hello'):  # st.button允许显示按钮小部件
    st.write('Why hello there')  # 点击按钮显示
else:
    st.write('Goodbye')  # 默认显示

st.header('st.write')

# Example 1

st.write('Hello, *World!* :sunglasses:')

# Example 2

st.write(1234)

# Example 3
df_data = pd.read_excel('审核列表 (17).xlsx')
# df_data = df_data[(df_data['溯源时间'] <= e_time[:-9]) & (df_data['溯源时间'] >= s_time[:-9])]  # 筛选时间内溯源排口
df_data['溯源类型'] = np.where(df_data['上报时间'] == df_data['溯源时间'], '新增排口溯源', '历史排口溯源')
# if keyword == 'fxpk':
#     df_data = df_data[(df_data["上报人"] != "王一虎")]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '谢军')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '童刚')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '梁凤祥')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '赵兵')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '高磊')]  # 奉贤和临港隔开
#     # df = df[(df["所属街镇"] != '四团镇')]  # 奉贤和临港隔开
if not df_data.empty:
    pt = df_data.pivot_table(
        values='UID',
        columns='溯源类型',
        index='上报人',
        fill_value='',
        aggfunc='count',
        margins=True,
        margins_name='合计'
    )
    pt.rename(columns={'溯源类型': ''}, inplace=True)  # 修改列名
st.write(pt)
df = pd.DataFrame({
    'first column': [1, 2, 3, 4],
    'second column': [10, 20, 30, 40]
})
st.write(df)

# Example 4

st.write('Below is a DataFrame:', df, 'Above is a dataframe.')

# Example 5

df2 = pd.DataFrame(
    np.random.randn(200, 3),
    columns=['a', 'b', 'c'])
c = alt.Chart(df2).mark_circle().encode(
    x='a', y='b', size='c', color='c', tooltip=['a', 'b', 'c'])
st.write(c)

st.header('st.slider')

# Example 1

st.subheader('Slider')

age = st.slider('How old are you?', 0, 130, 25)
st.write("I'm ", age, 'years old')

# Example 2

st.subheader('Range slider')

values = st.slider(
    'Select a range of values',
    0.0, 100.0, (25.0, 75.0))
st.write('Values:', values)

# Example 3

st.subheader('Range time slider')

appointment = st.slider(
    "Schedule your appointment:",
    value=(time(11, 30), time(12, 45)))
st.write("You're scheduled for:", appointment)

# Example 4

st.subheader('Datetime slider')

start_time = st.slider(
    "When do you start?",
    value=datetime(2020, 1, 1, 9, 30),
    format="MM/DD/YY - hh:mm")
st.write("Start time:", start_time)

st.header('Line chart')

chart_data = pd.DataFrame(
    np.random.randn(20, 3),
    columns=['a', 'b', 'c'])

st.line_chart(chart_data)
