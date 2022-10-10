import binascii
import codecs
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from Crypto.Cipher import DES
from Crypto.Util.Padding import pad
from gooey import Gooey, GooeyParser

from 排口平台字典 import pk_dict


@Gooey(
    richtext_controls=True,  # 打开终端对颜色支持
    language='chinese',
    header_show_title=False,
    program_name="普适排口-数据导出工具 v1.6",  # 程序名称
    encoding="utf-8",  # 设置编码格式，打包的时候遇到问题
    progress_regex=r"^(-?\d+)(\.\d+)?%$",
    # progress_expr="current / total * 100",
    timing_options={
        'show_time_remaining': True,
        'hide_time_remaining_on_complete': True, },
    default_size=(650, 555),
    # 再次执行，清除之前执行的日志
    clear_before_run=True,
)
def main():
    desc = f'工具说明:\n     本工具仅支持普适排口平台数据导出'
    my_cool_parser = GooeyParser(description=desc)

    my_cool_parser.add_argument('flagTrace', help='请选择数据类型', choices=['排查', '溯源'], default='溯源')

    my_cool_parser.add_argument('keyword', help='请选择要查询的项目',
                                choices=['浦东新区', '临港', '金山区', '青浦区', '松江区', '嘉定工业区', '嘉定跨界河',
                                         '崇明区', '奉贤区',
                                         '杨浦区', '闵行区'], default='奉贤区')

    my_cool_parser.add_argument('time_1', help='请选择查询开始日期', widget="DateChooser")
    my_cool_parser.add_argument('time_2', help='请选择查询结束日期', widget="DateChooser")
    my_cool_parser.add_argument('username', help='请输入用户名')
    my_cool_parser.add_argument('password', help='请输入密码')
    # 获取参数
    args = my_cool_parser.parse_args()

    print(args, flush=True)  # 坑点：flush=True在打包的时候会用到

    return args


def bs64_password():
    data = args.password
    data_obj = bytes(data, 'utf-8')
    key = '12345678'
    key_obj = bytes(key, 'utf-8')
    cipher = DES.new(key_obj, DES.MODE_ECB)
    ct = cipher.encrypt(pad(data_obj, 8))
    pwd = codecs.decode(binascii.b2a_base64(ct), 'UTF-8')
    pwd = pwd.strip('\n')
    # print(pwd)
    return pwd


def read_excel():
    df = pd.read_excel(filename)
    return df


def suyuan_data(i, o, time_1, time_2):
    data_info = {'outletUid': i}
    info = session.post(url3, json=data_info).json()['data']
    paicha = info['investigation']
    suyuan = info['trace']
    outletUid = i
    userName = info['userName']
    foundTime = '新增排口' if time_1 < info['foundTime'] and info['foundTime'] < time_2 else ''

    riverName = paicha['riverName']
    regionName = paicha['riverName']
    lon = paicha['lon']
    lat = paicha['lat']
    try:
        outletName = suyuan['Name']
    except KeyError:
        outletName = ''
    countryCode = suyuan['countryCode']
    traceReportStatus = info['traceReportStatus']
    if traceReportStatus == '120302':
        traceReportStatus = '审批通过'
    elif traceReportStatus == '120300':
        traceReportStatus = '待审核'
    else:
        traceReportStatus = '审批未通过'
    flagTraceDifficult = '是' if suyuan['flagTraceDifficult'] == 1 else '否'  # 是否是难点新增
    traceDifficultRemark = suyuan['traceDifficultRemark']
    if traceDifficultRemark != '' and flagTraceDifficult == '是':
        traceDifficultRemark = '普通溯源'
    elif traceDifficultRemark != '' and flagTraceDifficult == '否':
        traceDifficultRemark = '难点溯源'
    else:
        traceDifficultRemark = '普通溯源'
    Mark = suyuan['traceDifficultRemark']
    flagMixture = '否' if suyuan['flagMixture'] == '0' else '是'  # 是否是混排
    try:
        problemTypeValues = suyuan['problemTypeValues']  # 疑似排口问题
    except KeyError:
        problemTypeValues = ''
    problemTypeValueRemark = f'-{suyuan["problemTypeValueRemark"]}'  # 其他问题描述
    outletMaxType = f"{pk_dict[int(suyuan['outletMaxType'])]}" if suyuan['outletMaxType'] != '0' else ''
    outletMiddleType = f"-{pk_dict[int(suyuan['outletMiddleType'])]}" if suyuan['outletMiddleType'] != '0' else ''
    outletTypeRemark = f"-{suyuan['outletTypeRemark']}" if suyuan['outletTypeRemark'] != '' else ''
    pklb = (f'{outletMaxType}{outletMiddleType}{outletTypeRemark}')
    for i in problemTypeValues:
        if i == '342201':
            problemTypeValues = '未发现问题'
        elif i == '332403':
            problemTypeValues = '污水应纳未纳'
        elif i == '342205':
            problemTypeValues = '逃避监管私自设置的排污口'
        elif i == '342206':
            problemTypeValues = '排污口超标排放'
        elif i == '342207':
            problemTypeValues = '未实现雨污分流'
        elif i == '342202':
            problemTypeValues = '位于饮用水水源保护区'
        elif i == '342203':
            problemTypeValues = '处于生态保护红线、自然保护区等自然保护地范围内'
        else:
            problemTypeValues = '存在其他问题'
    problemTypeValues1 = problemTypeValues + problemTypeValueRemark
    number = len(suyuan['node'])
    try:
        unitName = suyuan['traceUnit'][1]['unitName'] if \
            suyuan['traceUnit'][1]['unitName'] == '' else \
            suyuan['traceUnit'][0]['unitName']  # 责任主体
    except IndexError:
        unitName = ''
    b = ['350104', '350105']
    logs = session.post(url4, json=data_info).json()['data']
    creatTime = ''
    for n in logs:
        logType = n['logType']
        if logType in b:
            userName = n['userName']
            creatTime = n['creatTime']  # 溯源时间
            pass
            df_suyuan.loc[o] = [userName, str(outletUid), creatTime, regionName,
                                riverName, outletName, lon, lat, countryCode, pklb, traceReportStatus,
                                flagTraceDifficult, Mark, foundTime,
                                traceDifficultRemark, flagMixture, problemTypeValues1, number, unitName]

    print('已完成 ' + '{:.2f}'.format((o + 1) / (len(uids)) * 100) + '%', flush=True)
    return df_suyuan


def suyuan_data_processing(time_1, time_2):
    df = df_suyuan
    df1 = df.sort_values(by=["溯源时间"], ascending=False)  # 降序
    df1 = df1.drop_duplicates(subset=['UID'], keep='first')  # 删除重复行，保留第一行
    df1 = df1.loc[df["是否混排"] != "出错"]  # 删除错误行
    if keyword1 == 'fxpk':
        df1 = df1[(df1["上报人"] != "王一虎")]  # 奉贤和临港隔开
        df1 = df1[(df1["上报人"] != '谢军')]  # 奉贤和临港隔开
        df1 = df1[(df1["上报人"] != '童刚')]  # 奉贤和临港隔开
        df1 = df1[(df1["上报人"] != '梁凤祥')]  # 奉贤和临港隔开
        df1 = df1[(df1["上报人"] != '赵兵')]  # 奉贤和临港隔开
        df1 = df1[(df1["上报人"] != '高磊')]  # 奉贤和临港隔开
        df1 = df1[(df1["所属街镇"] != '四团镇')]  # 奉贤和临港隔开
    df6 = df1[(df1['溯源时间'] < time_2) & (df1['溯源时间'] > time_1)]  # 筛选时间内溯源排口

    df2 = df6.loc[df["排口问题"] != "未发现问题-"]

    print(f'已完成数据重新排序：上报时间-降序', flush=True)
    time.sleep(1)
    print(f'已完成数据清洗：删除重复行，仅保留第一行数据', flush=True)
    time.sleep(1)
    print(f'最终数据总数：---{len(df)}---', flush=True)
    time.sleep(1)
    df_items = {}

    if not df6.empty:
        pt1 = df6.pivot_table(
            values='UID',
            columns='是否是新增排口',
            index='上报人',
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计'

        )

        pt1.rename(columns={'': '历史排口溯源'}, inplace=True)  # 修改列名
        pt1.rename(columns={'新增排口': '新增排口溯源'}, inplace=True)  # 修改列名
        # pt1.style.bar('历史排口溯源',vmin=10)
        df_items["筛选时间-是否是新增排口"] = pt1
    if not df6.empty:
        pt2 = df6.pivot_table(
            values='排口类型',
            # columns='排口类型',
            index='上报人',
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计'
        )
        pt2.rename(columns={'排口类型': '溯源总数'}, inplace=True)  # 修改列名
        df_items["筛选时间-溯源总数"] = pt2

    if not df6.empty:
        pt3 = df6.pivot_table(
            values='UID',
            columns='是否上报难点',
            index='上报人',
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计'
        )
        pt3.rename(columns={'是否上报难点': '新增难点数'}, inplace=True)  # 修改列名
        df_items["筛选时间-是否新增难点"] = pt3

    if not df6.empty:
        pt4 = df6.pivot_table(
            values='UID',
            columns='是否是难点溯源',
            index='上报人',
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计'
        )
        pt4.rename(columns={'是否是难点溯源': '难点溯源数'}, inplace=True)  # 修改列名
        df_items["筛选时间-溯源类型"] = pt4

    try:
        df3 = pd.merge(pt1, pt2, on='上报人', how='outer')  # 排口新增+溯源排口
        df4 = pd.merge(pt3, pt4, on='上报人', how='outer')  # 新增难点+难点溯源
        df3 = df3[['新增排口', '总溯源数量']]
        # if not pt4.empty:
        df4 = df4[['是', '难点溯源']]
        df5 = pd.merge(df3, df4, on='上报人', how='outer')
        df5.rename(columns={'是': '难点新增'}, inplace=True)
        df5 = df5[['新增排口', '难点新增', '难点溯源', '总溯源数量']]
        df_items["筛选时间-筛选结果"] = df5
    except KeyError:
        pass

    time_3 = time.strftime('%Y-%m-%d %H：%M：%S', time.localtime())
    file_name1 = f'./{keyword}溯源数据{time_3}.xlsx'

    with pd.ExcelWriter(file_name1) as f:
        df1.to_excel(f, sheet_name='源数据', index=False)
        df6.to_excel(f, sheet_name='筛选时间-源数据', index=False)
        df2.to_excel(f, sheet_name='筛选时间-问题排口汇总', index=False)
        for sheet_name, item in df_items.items():
            item.to_excel(f, sheet_name=sheet_name)
    print('已完成')
    print(f'已经保存文件：{file_name1}')


def paicha_data(i, o):
    outletUid = i
    data_info = {'outletUid': i}
    info = session.post(url3, json=data_info).json()['data']
    flagOutlet = info['flagOutlet=']
    if flagOutlet == '1':
        flagOutlet = '排口'
    else:
        flagOutlet = '非排口'
    paicha = info['investigation']
    suyuan = info['trace']
    userName = info['userName']
    foundTime = paicha['foundTime']
    riverName = paicha['riverName']
    riverCode = paicha['riverCode']
    regionName = paicha['regionName']
    lon = paicha['lon']
    lat = paicha['lat']
    investigateReportStatus = paicha['investigateReportStatus']
    if investigateReportStatus == '120302':
        investigateReportStatus = '审批通过'
    elif investigateReportStatus == '120300':
        investigateReportStatus = '待审核'
    else:
        investigateReportStatus = '审批未通过'
    flagSunny = paicha['flagSunny']  # 是否晴天排水
    if flagSunny == 1:
        flagSunny = '是'
    else:
        flagSunny = '否'
    try:
        cod = suyuan['detection']['cod']  # 快检-codmn
        nh3 = suyuan['detection']['nh3']  # 快检-氨氮
        ph = suyuan['detection']['ph']  # 快检-pH
        tp = suyuan['detection']['tp']  # 快检-总磷
        # 快检-照片
        imageUrl = suyuan['detection']['detectionPhoto'][0]['imageUrl']
    except KeyError:
        cod = ''
        nh3 = ''
        ph = ''
        tp = ''
        imageUrl = ''
    try:
        url1 = paicha['outletPhoto'][0]['imageUrl']
        url2 = paicha['outletPhoto'][1]['imageUrl']
    except IndexError:
        url1 = ''
        url2 = ''

    df_paicha.loc[j] = [
        userName, foundTime, riverName, riverCode, regionName, lon, lat, flagOutlet, outletUid,
        investigateReportStatus, flagSunny, cod, nh3, tp, ph, url1, url2, imageUrl
    ]

    print('已完成 ' + '{:.2f}'.format((j + 1) / (len(uids)) * 100) + '%', flush=True)
    return df_paicha


def paicha_data_processing():
    df = df_paicha
    df = df.loc[df["审批状态"] != "审批未通过"]
    df2 = df.loc[df["是否晴天排水"] != "否"]

    df_items = {}
    if not df.empty:
        pt1 = df.pivot_table(
            values='上报人',
            columns='是否晴天排水',
            index=['水系编码', '河湖名称'],
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计',
        )
        # pt1 = pd.merge(pt1, df2, on='水系编码', how='left')
        pt1.rename(columns={'上报人': '上报点数'}, inplace=True)  # 修改列名
        df_items["是否晴天排水"] = pt1
    if not df.empty:
        pt2 = df.pivot_table(
            values='排口临时编号',
            # columns='上报人',
            index=['水系编码', '河湖名称', '上报人', ],
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计',
        )
        pt2.rename(columns={'排口临时编号': '上报点数'}, inplace=True)  # 修改列名
        df_items["筛选结果2"] = pt2
    if not df.empty:
        pt3 = df.pivot_table(
            values='排口临时编号',
            aggfunc='count',
            columns='点位上报类别',
            index=['水系编码', '河湖名称'],
            fill_value=0,
            margins=True,
            margins_name='合计',
        )
        df_items["筛选结果3"] = pt3
    if not df.empty:
        pt4 = df.pivot_table(
            values='排口临时编号',
            columns='点位上报类别',
            index=['上报人'],
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计',
        )
        pt4 = pt4.rename(columns={'排口临时编号': '上报点数'}, inplace=False)
        df_items["筛选结果4"] = pt4
    if not df.empty:
        pt5 = df2.pivot_table(
            values='排口临时编号',
            # values='排口临时编号',
            # columns='上报人',
            # index='是否晴天排水',
            index=['水系编码', '河湖名称'],
            fill_value='',
            aggfunc='count',
            margins=True,
            margins_name='合计',
        )
        pt5 = pt5.rename(columns={'排口临时编号': '晴天排水数'}, inplace=False)
        df_items["晴天排水统计"] = pt5
    time_3 = time.strftime('%Y-%m-%d %H：%M：%S', time.localtime())
    file_name1 = f'./{keyword}排查数据{time_3}.xlsx'

    with pd.ExcelWriter(file_name1) as f:
        df.to_excel(f, sheet_name='源数据', index=False)
        for sheet_name, item in df_items.items():
            item.to_excel(f, sheet_name=sheet_name)
        df2.to_excel(f, sheet_name='晴天排水数据', index=False)
    print('已完成')
    print(f'已经保存文件：{file_name1}')


if __name__ == '__main__':
    args = main()
    keyword = args.keyword
    keyword1 = ''
    dic_1 = {
        '浦东新区': 310115, '金山区': 310116, '青浦区': 310118, '松江区': 310117, '嘉定工业区': 310114,
        '嘉定跨界河': 310114, '崇明区': 310151,
        '奉贤区': 310120, '杨浦区': 310110, '闵行区': 310112, '临港': 310115
    }
    dic_2 = {
        '浦东新区': '', '金山区': '', '青浦区': '', '松江区': '', '嘉定区': '', '崇明区': '',
        '奉贤区': '', '杨浦区': '', '闵行区': '闵行区'
    }
    if keyword == '浦东新区':
        keyword1 = 'pdpk'
    elif keyword == '临港':
        keyword1 = 'lgpk'
    elif keyword == '金山区':
        keyword1 = 'jspk'
    elif keyword == '青浦区':
        keyword1 = 'qppk'
    elif keyword == '松江区':
        keyword1 = 'sjpk'
    elif keyword == '嘉定工业区':
        keyword1 = 'jdpk-gyq'
    elif keyword == '嘉定跨界河':
        keyword1 = 'jdpk-kjh'
    elif keyword == '崇明区':
        keyword1 = 'cmpk'
    elif keyword == '奉贤区':
        keyword1 = 'fxpk'
    elif keyword == '杨浦区':
        keyword1 = 'yppk'
    elif keyword == '闵行区':
        keyword1 = ''

    time_1 = time.strftime(f"{args.time_1} 00:00:00", time.localtime())  # 开始时间
    time_2 = time.strftime(f"{args.time_2} 23:59:59", time.localtime())  # 截止时间
    flagTrace = args.flagTrace
    print(f'行政区：{keyword}\n开始时间：{time_1}\n结束时间：{time_2}\n数据类型：{flagTrace}')
    if flagTrace == '排查':
        flagTrace = 0
    else:
        flagTrace = 1
    t = time.time()
    url_1 = 'http://139.224.72.67:8081/ubipkapi/app/login'
    url_2 = 'http://139.224.72.67:8081/ubipkapi/outlet/approvalExport'
    url3 = 'http://139.224.72.67:8081/ubipkapi//outlet/outletDetailInfo'
    url4 = 'http://139.224.72.67:8081/ubipkapi/outlet/outletLogInfo'
    session = requests.session()
    filename = f'{t}tempfile.xlsx'
    data_1 = {
        'password': bs64_password(),
        'projectSerial': keyword1,
        'username': args.username
    }
    print(data_1)
    token = session.post(url_1, json=data_1).json()['data']['password']
    session.headers = {
        'Content-Type': 'application/json',
        'Host': '139.224.72.67:8081',
        'Referer': 'http://139.224.72.67:8081/ubipk/',
        'token': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36',
    }
    data = {
        'projectSerial': keyword1,
        'regionCode': dic_1[keyword],
        "flagTrace": flagTrace,
        "startTime": time_1,
        "endTime": time_2,
        "keyword": ""
    }
    a = session.post(url_2, json=data)
    with open(filename, 'wb') as f:
        f.write(a.content)
    df = read_excel()
    df = df.drop_duplicates(subset=['排口编号'], keep='first')  # 删除重复行，保留第一行
    uids = df['排口编号']
    os.remove(filename)

    header_suyuan = ['上报人', 'UID', '溯源时间', '所属街镇', '河湖名称', '排口名称', '经度', '纬度',
                     '排口正式编码',
                     '排口类型', '审批状态', '是否上报难点', '上报难点备注', '是否是新增排口', '是否是难点溯源',
                     '是否混排',
                     '排口问题',
                     '节点数量', '责任主体']
    header_paicha = [
        '上报人', '上报时间', '河湖名称', '水系编码', '所属街镇', '经度', '纬度', '点位上报类别', '排口临时编号',
        '审批状态',
        '是否晴天排水', 'COD', '氨氮', '总磷', 'pH', '排口照片1', '排口照片2', '快检照片'
    ]
    if flagTrace == 1:
        df_suyuan = pd.DataFrame(columns=header_suyuan, index=[])
        with ThreadPoolExecutor(50) as pool:
            o = 1
            for o, i in enumerate(uids):
                pool.submit(suyuan_data, i, o, time_1, time_2)
                o += 1
        suyuan_data_processing(time_1, time_2)
    else:
        df_paicha = pd.DataFrame(columns=header_paicha, index=[])
        with ThreadPoolExecutor(50) as pool:
            j = 1
            for o, i in enumerate(uids):
                pool.submit(paicha_data, i, j)
                j += 1
        paicha_data_processing()
