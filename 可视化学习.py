import pandas as pd
import streamlit as st

st.write('Hello world!')

df_data = pd.read_excel('审核列表 (17).xlsx')
# df_data = df_data[(df_data['溯源时间'] <= e_time[:-9]) & (df_data['溯源时间'] >= s_time[:-9])]  # 筛选时间内溯源排口
# df_data['溯源类型'] = np.where(df_data['上报时间'] == df_data['溯源时间'], '新增排口溯源', '历史排口溯源')
# if keyword == 'fxpk':
#     df_data = df_data[(df_data["上报人"] != "王一虎")]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '谢军')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '童刚')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '梁凤祥')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '赵兵')]  # 奉贤和临港隔开
#     df_data = df_data[(df_data["上报人"] != '高磊')]  # 奉贤和临港隔开
#     # df = df[(df["所属街镇"] != '四团镇')]  # 奉贤和临港隔开

pt = df_data.pivot_table(
    values='排口编号',
    columns='审批类型',
    index='填报人',
    fill_value='',
    aggfunc='count',
    margins=True,
    margins_name='合计'
)
pt.rename(columns={'溯源类型': ''}, inplace=True)  # 修改列名
st.dataframe(df_data)
st.dataframe(pt)
