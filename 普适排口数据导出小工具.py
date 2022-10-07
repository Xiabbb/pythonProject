import binascii
import codecs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    program_name="普适排口-数据导出工具 v1.5.1",  # 程序名称
    encoding="utf-8",  # 设置编码格式，打包的时候遇到问题
    # progress_regex=r"^progress: (?P<current>\d+)/(?P<total>\d+)$",  # 正则，用于模式化运行时进度信息
    default_size=(650, 555),
    progress_expr="current / total * 100",
    # 再次执行，清除之前执行的日志
    clear_before_run=True,
    timing_options={'show_time_remaining': True, 'hide_time_remaining_on_complete': False}
)
def main():
    desc = f'工具说明:\n     本工具仅支持普适排口平台数据导出'
    file_help_msg = "help..."
    my_cool_parser = GooeyParser(description=desc)

    my_cool_parser.add_argument('flagTrace', help='请选择数据类型', choices=['排查', '溯源'], default='溯源')

    my_cool_parser.add_argument('keyword', help='请选择要查询的项目',
                                choices=['浦东新区', '临港', '金山区', '青浦区', '松江区', '嘉定工业区', '嘉定跨界河',
                                         '崇明区', '奉贤区',
                                         '杨浦区', '闵行区'], default='奉贤区')
    # '闵行区-新虹街道项目', '闵行区-东川路街道项目', '闵行区-闵行市区管项目',
    # '闵行区-颛桥项目', '闵行区-浦江镇项目'], default='奉贤区')

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


def paicha_data(i):
    # print(i)
    lat = i['lat']
    lon = i['lon']
    name = i['userName']
    foundTime = i['foundTime']
    # gmtModified = i['gmtModified']
    # print(foundTime)
    riverName = i['riverName']
    reportType = i['reportType']
    if reportType == '120401':
        reportType = '排口新增'
    elif reportType == '120402':
        reportType = '排口新增'
    # elif reportType == '120403':
    #     reportType = '非排口新增'
    else:
        reportType = '非排口新增'
    outletUid = i['outletUid']
    regionName = i['regionName']
    reportStatus = i['reportStatus']
    if reportStatus == '120302':
        reportStatus = '审批通过'
    elif reportStatus == '120300':
        reportStatus = '待审核'
    else:
        reportStatus = '审批未通过'
    data_3 = {'outletUid': i['outletUid']}
    data = session.post(url3, json=data_3).json()['data']
    flagSunny = data['investigation']['flagSunny']  # 是否晴天排水
    if flagSunny == 1:
        flagSunny = '是'
    else:
        flagSunny = '否'
    riverCode = data['investigation']['riverCode']  # 水系编码
    try:
        url1 = data['investigation']['outletPhoto'][0]['imageUrl']
        url2 = data['investigation']['outletPhoto'][1]['imageUrl']
    except IndexError:
        url1 = ''
        url2 = ''
    try:
        cod = data['trace']['detection']['cod']  # 快检-codmn
        nh3 = data['trace']['detection']['nh3']  # 快检-氨氮
        ph = data['trace']['detection']['ph']  # 快检-pH
        tp = data['trace']['detection']['tp']  # 快检-总磷
        # 快检-照片
        imageUrl = data['trace']['detection']['detectionPhoto'][0]['imageUrl']
    except KeyError:
        cod = ''
        nh3 = ''
        ph = ''
        tp = ''
        imageUrl = ''

    return dict(zip(header_paicha, [
        name, foundTime, riverName, riverCode, regionName, lon, lat, reportType, outletUid,
        reportStatus, flagSunny, cod, nh3, tp, ph, url1, url2, imageUrl
    ]))


def paicha():
    df = pd.DataFrame(columns=header_paicha, index=[])
    task_list = []
    with ThreadPoolExecutor(max_workers=20) as executor:

        for p in info:
            for i in p:
                task_list.append(executor.submit(paicha_data, i))

        for future in as_completed(task_list):
            # future.add_done_callback(lambda future: pbar.update(1))
            data = future.result()
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            df = df.sort_values(by=["上报时间"], ascending=False)  # 降序

            df = df.drop_duplicates(subset=['排口临时编号'], keep='first')  # 删除重复行，保留第一行
            df = df.loc[df["审批状态"] != "审批未通过"]
            if keyword1 == 'fxpk':
                df = df[(df["上报人"] != "王一虎")]  # 奉贤和临港隔开
                df = df[(df["上报人"] != '谢军')]  # 奉贤和临港隔开
                df = df[(df["上报人"] != '童刚')]  # 奉贤和临港隔开
                df = df[(df["上报人"] != '梁凤祥')]  # 奉贤和临港隔开
                df = df[(df["上报人"] != '赵兵')]  # 奉贤和临港隔开
                df = df[(df["上报人"] != '高磊')]  # 奉贤和临港隔开
                df = df[(df["所属街镇"] != '四团镇')]  # 奉贤和临港隔开
            df2 = df.loc[df["是否晴天排水"] != "否"]
        print(f'已完成数据重新排序：上报时间-降序', flush=True)
        time.sleep(1)
        print(f'已完成数据清洗：删除重复行，仅保留第一行数据', flush=True)
        time.sleep(1)
        print(f'已删除（排查数据）：审批状态 = “未通过” 的数据\n最终数据总数：---{len(df)}---', flush=True)
        time.sleep(1)

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
        file_name = f'./{keyword}排查数据{time_3}.xlsx'

        with pd.ExcelWriter(file_name) as f:
            df.to_excel(f, sheet_name='源数据', index=False)
            for sheet_name, item in df_items.items():
                item.to_excel(f, sheet_name=sheet_name)
            df2.to_excel(f, sheet_name='晴天排水数据', index=False)
        print('已完成')
        print(f'已经保存文件：{file_name}')


def suyuan_data(i):
    # 排口上报时间
    foundTime = '新增排口' if time_1 < i['foundTime'] and i['foundTime'] < time_2 else ''
    userName = i['userName']  # 上报人
    outletUid = i['outletUid']  # UID

    # gmtModified = i['gmtModified']  # 溯源时间
    regionName = i['regionName']  # 所属街镇
    riverName = i['riverName']  # 河湖名称
    outletName = i['outletName']  # 排口名称
    lon = i['lon']
    lat = i['lat']
    data_3 = {
        'outletUid': outletUid
    }
    data = session.post(url3, json=data_3).json()['data']
    # print(data_3)
    traceReportStatus = data['traceReportStatus']  # 审批状态
    if traceReportStatus == '120302':
        traceReportStatus = '审批通过'
    elif traceReportStatus == '120300':
        traceReportStatus = '待审核'
    else:
        traceReportStatus = '审批未通过'
    countryCode = data['trace']['countryCode']  # 排口正式编码
    # problemTypeValueRemark = data['trace']['problemTypeValueRemark']  # 其他问题描述
    try:
        problemTypeValues = data['trace']['problemTypeValues']  # 疑似排口问题
        problemTypeValueRemark = '-' + data['trace']['problemTypeValueRemark']  # 其他问题描述

    except KeyError:
        problemTypeValues = ''
        problemTypeValueRemark = ''
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
    # 排口大类
    d = f"{pk_dict[int(data['trace']['outletMaxType'])]}-" if data['trace']['outletMaxType'] != '0' else ''

    x = pk_dict[int(data['trace']['outletMiddleType'])
    ] if data['trace']['outletMiddleType'] != '0' else ''  # 排口小类

    # 排口类别备注
    m = f"-{data['trace']['outletTypeRemark']}" if data['trace']['outletTypeRemark'] != '' else ''

    pklb = (f'{d}{x}{m}')  # 拼接排口类型
    try:
        flagMixture = '否' if data['trace']['flagMixture'] == '0' else '是'  # 是否是混排
    except KeyError:
        flagMixture = '出错'
    # 是否是难点新增
    flagTraceDifficult = '是' if data['trace']['flagTraceDifficult'] == 1 else '否'

    # 是否是难点溯源-难点备注不为空
    traceDifficultRemark = data['trace']['traceDifficultRemark']
    if traceDifficultRemark != '' and flagTraceDifficult == '是':
        traceDifficultRemark = '普通溯源'
    elif traceDifficultRemark != '' and flagTraceDifficult == '否':
        traceDifficultRemark = '难点溯源'
    else:
        traceDifficultRemark = '普通溯源'

    Mark = data['trace']['traceDifficultRemark']  # 难点新增备注说明

    number = len(data['trace']['node'])
    try:
        unitName = data['trace']['traceUnit'][1]['unitName'] \
            if data['trace']['traceUnit'][0]['unitName'] == '' else \
            data['trace']['traceUnit'][0]['unitName']  # 责任主体

    except IndexError:
        unitName = ''
    data_4 = {
        'outletUid': outletUid
    }
    info_list = session.post(url4, json=data_4).json()['data']
    creatTime = ''
    b = ['350104', '350105']
    for i in info_list:
        logType = i['logType']
        # if logType != '350105' and logType != '350104' and logType != '350106':
        if logType in b:
            creatTime = i['creatTime']
            pass

    return dict(zip(header_suyuan, [userName, outletUid, creatTime, regionName,
                                    riverName, outletName, lon, lat, countryCode, pklb, traceReportStatus,
                                    flagTraceDifficult,
                                    Mark,
                                    foundTime,
                                    traceDifficultRemark, flagMixture, problemTypeValues1, number, unitName]))


def suyuan():
    df = pd.DataFrame(columns=header_suyuan, index=[])
    task_list = []
    with ThreadPoolExecutor(max_workers=20) as executor:

        for p in info:
            for i in p:
                if i['outletUid'][0] != '2':
                    continue
                task_list.append(executor.submit(suyuan_data, i))

        for future in as_completed(task_list):
            # future.add_done_callback(lambda future: pbar.update(1))
            data = future.result()
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
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
        file_name = f'./{keyword}溯源数据{time_3}.xlsx'

        with pd.ExcelWriter(file_name) as f:
            df1.to_excel(f, sheet_name='源数据', index=False)
            df6.to_excel(f, sheet_name='筛选时间-源数据', index=False)
            df2.to_excel(f, sheet_name='筛选时间-问题排口汇总', index=False)
            for sheet_name, item in df_items.items():
                item.to_excel(f, sheet_name=sheet_name)
        print('已完成')
        print(f'已经保存文件：{file_name}')


if __name__ == '__main__':

    # <editor-fold desc="垃圾代码">
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
    # elif keyword == '闵行区-东川路街道项目':
    #     keyword1 = 'mhpk-dcljd'
    # elif keyword == '闵行区-闵行市区管项目':
    #     keyword1 = 'mhpk-mhsqg'
    # elif keyword == '闵行区-颛桥项目':
    #     keyword1 = 'mhpk-zq'
    # elif keyword == '闵行区-浦江镇项目':
    #     keyword1 = 'mhpk-pjz'
    # </editor-fold>
    time_1 = time.strftime(f"{args.time_1} 00:00:00", time.localtime())  # 开始时间
    time_2 = time.strftime(f"{args.time_2} 23:59:59", time.localtime())  # 截止时间
    flagTrace = args.flagTrace
    print(f'行政区：{keyword}\n开始时间：{time_1}\n结束时间：{time_2}\n数据类型：{flagTrace}')
    if flagTrace == '排查':
        flagTrace = 0
    else:
        flagTrace = 1
    # username = args.username
    # password = args.password
    # data = args.password
    # data_obj = bytes(data, 'utf-8')
    # key = '12345678'
    # key_obj = bytes(key, 'utf-8')
    # cipher = DES.new(key_obj, DES.MODE_ECB)
    # ct = cipher.encrypt(pad(data_obj, 8))
    # password = codecs.decode(binascii.b2a_base64(ct), 'UTF-8')

    session = requests.Session()
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
    url1 = 'http://139.224.72.67:8081/ubipkapi/app/login'
    data_1 = {
        'password': bs64_password(),
        'projectSerial': keyword1,
        'username': args.username
    }
    print(data_1)
    token = session.post(url1, json=data_1).json()['data']['password']
    # <editor-fold desc="绑定信息">

    url2 = 'http://139.224.72.67:8081/ubipkapi//outlet/reportInfo'
    session.headers = {
        'Content-Type': 'application/json',
        'Host': '139.224.72.67:8081',
        'Referer': 'http://139.224.72.67:8081/ubipk/',
        'token': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36',
    }
    a = 0
    k = ''
    infos = []
    for a in range(20000):
        a += 1
        print(f'正在完成第{a}次数据请求，请稍后。', flush=True)
        data_2 = {
            # 筛选结束时间
            'endTime': time_2,
            'regionCode': dic_1[keyword],
            'flagTrace': flagTrace,  # 0排查，1溯源
            'approvalStatus': "",
            # 筛选区域
            'keyword': '',
            'pageIndex': a,
            # 设置获取数量，'0'为最大值
            'pageSize': 1000,
            # 筛选开始时间
            'startTime': time_1
        }
        res = session.post(url2, json=data_2).json()['data']['list']
        n = len(res)
        print(f'已成功获取{n}条数据')
        infos.append(res)

        if len(res) < 1000:
            break

    if a < 2:
        k = 0
    else:
        k = a - 1
    info = infos

    print(f'总计获取数据：  {k * 1000 + n}   条')
    url3 = 'http://139.224.72.67:8081/ubipkapi//outlet/outletDetailInfo'
    url4 = 'http://139.224.72.67:8081/ubipkapi/outlet/outletLogInfo'

    print('下载完毕，数据处理中，请稍后。。。')
    if flagTrace == 1:
        suyuan()
    else:
        paicha()
