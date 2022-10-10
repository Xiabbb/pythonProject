import datetime
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import time
from concurrent.futures.thread import ThreadPoolExecutor

st.set_page_config(page_title='溯源数据')
st.title('溯源数据分析')


def read_excel():
    df = pd.read_excel(filename)
    return df


def uids(username, password, keyword, flagTrace, ):
    data_1 = {
        "username": username,
        "password": password,
        "projectSerial": dic_2[keyword]
    }
    token = session.post(url_1, json=data_1).json()['data']['password']
    session.headers = {
        'Content-Type': 'application/json',
        'Host': '139.224.72.67:8081',
        'Referer': 'http://139.224.72.67:8081/ubipk/',
        'token': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36',
    }
    data = {
        "flagTrace": flagTrace,
        "startTime": s_time1,
        "endTime": e_time1,
        "keyword": ""
    }
    a = session.post(url_2, json=data)
    with open(filename, 'wb') as f:
        f.write(a.content)
    df = read_excel()
    df = df.drop_duplicates(subset=['排口编号'], keep='first')  # 删除重复行，保留第一行
    uids = df['排口编号']
    return uids


def uid(i, o):
    data_4 = {'outletUid': i}
    try:
        info_list = session.post(url4, json=data_4).json()['data']
        userName = ''
        creatTime = ''
        b = ['350104', '350105']
        foundtime = info_list[0]['creatTime'][:-9]  # 排查时间
        for n in info_list:
            logType = n['logType']
            if logType in b:
                userName = n['userName']
                creatTime = n['creatTime'][:-9]  # 溯源时间
                pass
            df_suyuan.loc[o] = [i, userName, foundtime, creatTime]

    except KeyError:
        pass
    return df_suyuan


if __name__ == '__main__':
    with st.spinner('数据获取中，请稍后...'):
        bar = st.progress(0)
        t = time.time()
        url_1 = 'http://139.224.72.67:8081/ubipkapi/app/login'
        url_2 = 'http://139.224.72.67:8081/ubipkapi/outlet/approvalExport'
        url3 = 'http://139.224.72.67:8081/ubipkapi//outlet/outletDetailInfo'
        url4 = 'http://139.224.72.67:8081/ubipkapi/outlet/outletLogInfo'
        session = requests.session()
        filename = f'./tempfile.xlsx'
        dic_1 = {
            '浦东新区': 310115, '金山区': 310116, '青浦区': 310118, '松江区': 310117, '嘉定工业区': 310114,
            '崇明区': 310151,
            '奉贤区': 310120, '嘉定跨界河': 310114, '闵行区': 310112, '临港': 310115
        }
        dic_2 = {
            '浦东新区': 'pdpk', '金山区': 'jspk', '青浦区': 'qppk', '松江区': 'sjpk', '嘉定工业区': 'jdpk-gyq',
            '崇明区': 'cmpk',
            '奉贤区': 'fxpk', '嘉定跨界河': 'jdpk-kjh'
        }

        center = {'fxpk': [121.55843505, 30.92692355],
                  'jspk': [121.23756409, 30.83031866],
                  'sjpk': [121.23756409, 31.02763617],
                  'qppk': [121.08306885, 31.13525236],
                  'jdpk-gyq': [121.24786377, 31.36184585],
                  'jdpk-kjh': [121.24786377, 31.36184585],
                  'cmpk': [121.55136108, 31.61596594],
                  }
        df_info_headers = ['UID', '上报人', '上报时间', '溯源时间']
        df_suyuan = pd.DataFrame(columns=df_info_headers, index=[])
        flagTrace = st.radio('数据类型', ['排查', '溯源'], horizontal=True, index=1)
        keyword = st.radio('查询区域', ['浦东新区', '临港', '金山区', '青浦区', '松江区', '嘉定工业区', '嘉定跨界河',
                                        '崇明区', '奉贤区', ], horizontal=True, index=6)
        username = f'xiabinbin'
        password = f'0UXUWLF4bXw='
        t1, t2 = st.columns(2)
        t1 = t1.date_input('选择开始日期')
        t2 = t2.date_input('选择截止日期')
        if flagTrace == '排查':
            flagTrace = 0
        else:
            flagTrace = 1
        s_time1 = f'{t1} {datetime.datetime.strftime(datetime.datetime.now(), "00:00:00")}'  # 开始时间

        e_time1 = f'{t2} {datetime.datetime.strftime(datetime.datetime.now(), "23:59:59")}'  # 截止时间
        s_time = datetime.datetime.strptime(s_time1, '%Y-%m-%d %H:%M:%S')
        e_time = datetime.datetime.strptime(e_time1, '%Y-%m-%d %H:%M:%S')
        uid_list = uids(username, password, keyword, flagTrace)

        with ThreadPoolExecutor(50) as pool:
            o = 1
            for i in uid_list:
                pool.submit(uid, i, o)
                bar.progress(o / len(uid_list))
                o += 1
    df_data = df_suyuan

    df_data['溯源时间'] = pd.to_datetime(df_data['溯源时间'])
    df_data = df_data[(df_data['溯源时间'] <= str(t2)) & (df_data['溯源时间'] >= str(t1))]  # 筛选时间内溯源排口
    df_data['溯源类型'] = np.where(df_data['上报时间'] == df_data['溯源时间'], '新增排口溯源', '历史排口溯源')
    # df_data.to_excel(filename)
    if dic_2[keyword] == 'fxpk':
        df_data = df_data[(df_data["上报人"] != "王一虎")]  # 奉贤和临港隔开
        df_data = df_data[(df_data["上报人"] != '谢军')]  # 奉贤和临港隔开
        df_data = df_data[(df_data["上报人"] != '童刚')]  # 奉贤和临港隔开
        df_data = df_data[(df_data["上报人"] != '梁凤祥')]  # 奉贤和临港隔开
        df_data = df_data[(df_data["上报人"] != '赵兵')]  # 奉贤和临港隔开
        df_data = df_data[(df_data["上报人"] != '高磊')]  # 奉贤和临港隔开
        # df = df[(df["所属街镇"] != '四团镇')]  # 奉贤和临港隔开
    if not df_data.empty:
        pt = df_data.pivot_table(
            values='UID',
            columns='溯源类型',
            index='上报人',
            fill_value=0,
            aggfunc='count',
            margins=True,
            margins_name='合计'
        )
        pt.rename(columns={'溯源类型': ''}, inplace=True)  # 修改列名
        # pt.rename(columns={'新增排口': '新增排口溯源'}, inplace=True)  # 修改列名
    table = pt.reset_index()

    table1 = table.loc[table["上报人"] != "合计"]  # 删除错误行
    table1 = table1.sort_values(by=["合计"], ascending=False)
    num = len(table['上报人']) * 40
    if num < 1000:
        num = 1000
    # 绘制柱状图, 配置相关参数
    bar_chart = px.bar(table1,
                       height=num,
                       width=420,
                       x='合计',
                       y='上报人',
                       text='合计',
                       color_discrete_sequence=['#F63366'] * len(table),
                       template='plotly_white')

    chart_data = pd.DataFrame(table1)
    df = pd.read_excel(filename)
    df1 = pd.DataFrame(df, columns=['排口经度', '排口纬度'])
    df1.rename(columns={'排口经度': 'lon'}, inplace=True)
    df1.rename(columns={'排口纬度': 'lat'}, inplace=True)
    st.map(df1)
    s1, s2 = st.columns(2)
    s1.dataframe(table, height=num)
    s2.plotly_chart(bar_chart)
