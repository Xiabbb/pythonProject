import binascii
import codecs
import os
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests
from Crypto.Cipher import DES
from Crypto.Util.Padding import pad
from pyecharts import options as opts
from pyecharts.charts import Bar, Map, Geo
from pywebio import start_server
from pywebio.input import TEXT, actions, input_group, radio, input
from pywebio.output import put_html, put_markdown, put_processbar, put_row, put_text, set_processbar, \
    popup, PopupSize


def set_now_ts(set_value):
    set_value(time.strftime('%Y-%m-%d', time.localtime()))


def bs64_password(password):
    data = password
    data_obj = bytes(data, 'utf-8')
    key = '12345678'
    key_obj = bytes(key, 'utf-8')
    cipher = DES.new(key_obj, DES.MODE_ECB)
    ct = cipher.encrypt(pad(data_obj, 8))
    pwd = codecs.decode(binascii.b2a_base64(ct), 'UTF-8')
    pwd = pwd.strip('\n')
    return pwd


def read_excel():
    df = pd.read_excel(filename)
    return df


def uids(username, password, keyword, flagTrace, s_time, e_time):
    data_1 = {
        "username": username,
        "password": password,
        "projectSerial": keyword
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
        'projectSerial': keyword,
        'regionCode': dic_1[keyword],
        "flagTrace": flagTrace,
        "startTime": s_time,
        "endTime": e_time,
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


def main():
    put_processbar('bar')
    s = time.time()
    global pt

    # put_row([
    #     put_image(
    #         'https://gimg2.baidu.com/image_search/src=http%3A%2F%2Fcdnq.duitang.com%2Fuploads%2Fitem%2F201411%2F24%2F20141124182414_ZaN8w.gif&refer=http%3A%2F%2Fcdnq.duitang.com&app=2002&size=f9999,10000&q=a80&n=0&g=0n&fmt=auto?sec=1667275119&t=377c367770143913aafd0e2a832b1203',
    #         width='100px'),
    #     put_image(
    #         'https://gimg2.baidu.com/image_search/src=http%3A%2F%2Fcdnq.duitang.com%2Fuploads%2Fitem%2F201411%2F24%2F20141124182414_ZaN8w.gif&refer=http%3A%2F%2Fcdnq.duitang.com&app=2002&size=f9999,10000&q=a80&n=0&g=0n&fmt=auto?sec=1667275119&t=377c367770143913aafd0e2a832b1203',
    #         width='100px'),
    #     put_image(
    #         'https://gimg2.baidu.com/image_search/src=http%3A%2F%2Fcdnq.duitang.com%2Fuploads%2Fitem%2F201411%2F24%2F20141124182414_ZaN8w.gif&refer=http%3A%2F%2Fcdnq.duitang.com&app=2002&size=f9999,10000&q=a80&n=0&g=0n&fmt=auto?sec=1667275119&t=377c367770143913aafd0e2a832b1203',
    #         width='100px')
    # ])

    data = input_group('用户数据', [
        input('用户名ID', name='username', type=TEXT),
        input('密码', name='password', type=TEXT),
        input('开始日期', name='s_time', type=TEXT, action=('当前日期', set_now_ts),
              placeholder="点击右边就能看到现在时间哦~"),
        input('截止日期', name='e_time', type=TEXT, action=('当前日期', set_now_ts),
              placeholder="点击右边就能看到现在时间哦~"),
        radio("请大佬选择：", ["排查", "溯源"], name='flagTrace', inline=True, value='溯源'),
        actions('筛选区域', [
            {'label': '浦东新区', 'value': 'pdpk'},
            {'label': '临港', 'value': 'lgpk'},
            {'label': '金山区', 'value': 'jspk'},
            {'label': '青浦区', 'value': 'qppk'},
            {'label': '松江区', 'value': 'sjpk'},
            {'label': '嘉定工业区', 'value': 'jdpk-gyq'},
            {'label': '嘉定跨界河', 'value': 'jdpk-kjh'},
            {'label': '崇明区', 'value': 'cmpk'},
            {'label': '奉贤区', 'value': 'fxpk', },
        ], name='keyword', help_text='请选择区域'),
    ])
    username = data['username']
    password = bs64_password(data['password'].strip())
    keyword = data['keyword']
    flagTrace = data['flagTrace']
    if flagTrace == '排查':
        flagTrace = 0
    else:
        flagTrace = 1
    s_time = time.strftime(f"{data['s_time']} 00:00:00", time.localtime())  # 开始时间
    e_time = time.strftime(f"{data['e_time']} 23:59:59", time.localtime())  # 截止时间
    uid_list = uids(username, password, keyword, flagTrace, s_time, e_time)

    # ===================================================线程池====================================================
    with ThreadPoolExecutor(50) as pool:
        o = 1
        for i in uid_list:
            pool.submit(uid, i, o)
            set_processbar('bar', o / len(uid_list))
            o += 1
    # ===========================================================================================================

    df_data = df_suyuan
    df_data = df_data[(df_data['溯源时间'] <= e_time[:-9]) & (df_data['溯源时间'] >= s_time[:-9])]  # 筛选时间内溯源排口
    df_data['溯源类型'] = np.where(df_data['上报时间'] == df_data['溯源时间'], '新增排口溯源', '历史排口溯源')
    if keyword == 'fxpk':
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
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计'
        )
        pt.rename(columns={'溯源类型': ''}, inplace=True)  # 修改列名
        # pt.rename(columns={'新增排口': '新增排口溯源'}, inplace=True)  # 修改列名
    table = pt.reset_index()
    put_row([put_markdown('## 溯源-数据分析'),
             put_text(f'\n开始时间：{s_time}\n截止时间：{e_time}')])
    # =================================================地图==========================================================
    df = read_excel()
    uidd = df['排口编号']
    name = df['填报人']
    lon = df['排口经度']
    lat = df['排口纬度']
    datas = [(str(i), j, k) for i, j, k in zip(uidd.values, lon.values, lat.values)]
    datass = [(str(i), j) for i, j, in zip(uidd.values, name.values)]

    d = (
        Map()
        .add(series_name="商家A", data_pair=datass, maptype="上海")
        .set_global_opts(
            title_opts=opts.TitleOpts(title="Map-VisualMap（连续型）"),
            visualmap_opts=opts.VisualMapOpts(max_=200, range_color=["#90CAF9", "#1E88E5", "#0D47A1"]),
        )
    )
    d.width = "100%"
    # put_html(d.render_notebook())

    c = Geo(is_ignore_nonexistent_coord=True, )
    c.add_schema(maptype="上海", zoom=5, center=center[keyword])
    for i in datas:
        c.add_coordinate(i[0], i[1], i[2])
    c.add("排口点位", data_pair=datass, color="red", symbol_size=6, is_large=True, )
    c.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    c.set_global_opts(
        visualmap_opts=opts.VisualMapOpts(is_show=False, max_=200, range_color=["#90CAF9", "#1E88E5", "#0D47A1"]),
        title_opts=opts.TitleOpts(title="溯源点位分布图")
    )
    c.width = "100%"
    put_html(c.render_notebook())
    # ==============================================================================================================
    put_html(table.to_html())

    table = table.loc[table["上报人"] != "合计"]  # 删除错误行
    table = table.sort_values(by=["合计"], ascending=False)
    num = len(table['上报人']) * 18
    if num < 1000:
        num = 1000
    # =================================================图表==========================================================
    c = (
        Bar(init_opts=opts.InitOpts(height=f"{num}px", bg_color='white', page_title='普适排口溯源数据分析'))
        .add_xaxis(table['上报人'].tolist())
        .add_yaxis("上报数量", table['合计'].tolist(), bar_max_width=50)
        .reversal_axis()
        .set_series_opts(label_opts=opts.LabelOpts(position="right"), markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="min", name="最小值"),
                opts.MarkPointItem(type_="average", name="平均值"),
                opts.MarkPointItem(type_="max", name="最大值"),
            ]
        ),
                         )
        .set_global_opts(
            title_opts=opts.TitleOpts(
                title="人均工作量统计"
            ),
            yaxis_opts=opts.AxisOpts(
                splitline_opts=opts.SplitLineOpts(
                    is_show=True,
                    linestyle_opts=opts.LineStyleOpts(
                        type_='dotted'
                    )
                ),
                min_interval=0,
                max_interval=num
            ),
            xaxis_opts=opts.AxisOpts(
                splitline_opts=opts.SplitLineOpts(
                    is_show=True,
                    linestyle_opts=opts.LineStyleOpts(type_='dotted')
                ),
                min_interval=1,
            ),
            toolbox_opts=opts.ToolboxOpts(
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(
                        is_show=True,
                        type_='png',
                        background_color='auto'
                    ),
                    restore=opts.ToolBoxFeatureRestoreOpts(
                        is_show=False
                    ),
                    data_view=opts.ToolBoxFeatureDataViewOpts(
                        is_show=False
                    ),
                    data_zoom=opts.ToolBoxFeatureDataZoomOpts(
                        is_show=False
                    ),
                    magic_type=opts.ToolBoxFeatureMagicTypeOpts(
                        is_show=False
                    )
                )
            )
        )
    )

    put_html(c.render_notebook())
    e = time.time()
    ss = e - s
    popup('已完成！\n用时统计', f'总计用时 {ss} ', size=PopupSize.SMALL, implicit_close=True)
    # ==============================================================================================================
    os.remove(filename)


if __name__ == '__main__':
    t = time.time()
    url_1 = 'http://139.224.72.67:8081/ubipkapi/app/login'
    url_2 = 'http://139.224.72.67:8081/ubipkapi/outlet/approvalExport'
    url3 = 'http://139.224.72.67:8081/ubipkapi//outlet/outletDetailInfo'
    url4 = 'http://139.224.72.67:8081/ubipkapi/outlet/outletLogInfo'
    session = requests.session()
    filename = f'{t}tempfile.xlsx'
    dic_1 = {
        'pdpk': 310115, 'jspk': 310116, 'qppk': 310118, 'sjpk': 310117, 'jdpk-gyq': 310114, 'cmpk': 310151,
        'fxpk': 310120, 'jdpk-kjh': 310114, '闵行区': 310112, 'lgpk': 310115
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

    start_server(main, port=8081, cdn=False, debug=True)
