import codecs

import binascii
import requests
from Crypto.Cipher import DES
from Crypto.Util.Padding import pad


def bs64_password():
    data = 密码
    data_obj = bytes(data, 'utf-8')
    key = '12345678'
    key_obj = bytes(key, 'utf-8')
    cipher = DES.new(key_obj, DES.MODE_ECB)
    ct = cipher.encrypt(pad(data_obj, 8))
    pwd = codecs.decode(binascii.b2a_base64(ct), 'UTF-8')
    pwd = pwd.strip('\n')
    return pwd


def addUser():  # 新建账号
    addUser = session.post(
        url='http://139.224.72.67:8081/ubipkapi/ubiUser/addUser',
        json={"name": 账号,
              "userName": 姓名,
              "phone": "18888888888",
              "employeeNumber": "001",
              "type": 100204,
              "password": bs64_password(),
              "defaultRoute": "home",
              "projectTitle": 姓名,
              "expirationTime": 过期时间}
    )
    a = addUser.json()['msg']
    if a == 'success':
        a = '账号建立成功！'
    print(a)


def selectUser():  # 查询账号ID
    selectUser = session.post(
        url='http://139.224.72.67:8081/ubipkapi/ubiUser/selectUser',
        json={"userId": 账号, "userName": ''},
    )
    id = selectUser.json()['data'][0]['id']
    print(f'id:{id}')
    return id


def bindUgroupRelation():  # 赋权
    bindUgroupRelation = session.post(
        url='http://139.224.72.67:8081/ubipkapi/gbac/bindUgroupRelation',
        json={"userId": 2292, "ugroupIds": ["108", "555"]},
    )
    a = bindUgroupRelation.json()['msg']
    if a == 'success':
        a = '赋权成功！'
    print(a)


def deleteUserById():  # 删除账号
    deleteUserById = session.post(
        url='http://139.224.72.67:8081/ubipkapi/gbac/deleteUserById',
        json={"userId": selectUser()},
    )
    a = deleteUserById.json()
    if a == 'success':
        a = '账号删除成功！'
    print(a)


if __name__ == '__main__':
    session = requests.session()
    login = session.post(url='http://139.224.72.67:8081/ubipkapi/app/login',
                         json={"username": "psadmin", "password": "X1/SBooAoUH+uVm31GQvyw==", "projectSerial": "fxpk"}
                         )
    token = login.json()['data']['password']
    session.headers = {
        'Host': '139.224.72.67:8081',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
        'Referer': 'http://139.224.72.67:8081/ubipk/',
        'token': token,
        'Origin': 'http://139.224.72.67:8081',
        "Content-Type": "application/json"

    }
    账号 = 'tttttttt'
    密码 = '123456'
    姓名 = '测试测试测试测试'
    过期时间 = '2023-09-28 23:59:59'

    # addUser()   #新建账号
    # bindUgroupRelation()  # 赋权
    # deleteUserById()  # 删除账号
