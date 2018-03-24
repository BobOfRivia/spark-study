import requests
import re

stackUrl = "http://quote.eastmoney.com/stocklist.html"



def gethtml(url:str):
    html = requests.get(url)
    html.encoding = 'gb2312'
    return html.text

def getcodeList(html:str):
    reg = r'<li><a target="_blank" href="http://quote.eastmoney.com/\S\S(.*?).html">(.*?)\('
    pat = re.compile(reg)
    code = pat.findall(html)
    return code

# [('201000', 'R003'), ('201001', 'R007')]
def writeToDB(list):
    import pymysql
    import sys
    db = pymysql.connect(host='localhost',port=3306,user='root',password='541325',database='lifeblog',charset='utf8')
    batchsql = 'insert into t_stock_list values (\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')'
    belongType = 'sh'
    for code,name in list:
        if(code == '000001'):
            belongType='sz'
        db.cursor().execute(batchsql.format(code, name, 1, '0',belongType))

    db.commit()
    db.close()


if __name__ == '__main__':
    list = getcodeList(gethtml(stackUrl))
    writeToDB(list)


