import xlrd
from xlrd import xldate_as_tuple
import datetime
import mysql.connector as connt
import requests
import json

weekdict = {
    "一":1,
    "二":2,
    "三":3,
    "四":4,
    "五":5,
    "六":6,
    "日":7,
}

data1=xlrd.open_workbook("/Users/liangye/Downloads/ssq.xls")
table=data1.sheets()[0]

tables=[]

def import_excel(excel,conn):
    countflag='2013066'
    for rown in range(excel.nrows):
        dateidstr=""
        try:
            dateidstr=fill_dateid(str(int(table.cell_value(rown,1))))
            add_ssq(conn,dateidstr,'null',table.cell_value(rown,9),table.cell_value(rown,10),table.cell_value(rown,11),table.cell_value(rown,12),table.cell_value(rown,13),table.cell_value(rown,14),int(table.cell_value(rown,15)),'null','null','null','null','null','null','null',0,'null','null','null')
        except Exception:
            print('convert content is fail! don`t import data to database')
        if countflag==dateidstr:
            break
    
def ssq_connect(user,pwd,host,database):
    con=None
    try:
        con=connt.connect(user=user,password=pwd,host=host,database=database)
    except Exception:
        print('op database is fail!')
        exit(1)
    else:
        return con

def add_ssq(conn,dateid,windate,winnumber1,winnumber2,winnumber3,winnumber4,winnumber5,winnumber6,winnumber7,sellprice,wins1,price1,wins2,price2,wins3,price3,sunday,sales,poolmoney,memo):
    mycursor=conn.cursor()

    sqlstr="insert into number_info(dateid, windate, winnumber1, winnumber2, winnumber3, winnumber4, winnumber5, winnumber6, winnumber7, sellprice, wins1, price1, wins2, price2, wins3, price3, sunday,sales,poolmoney,memo) values ("+str(dateid)+",'"+str(windate)+"',"+str(winnumber1)+","+str(winnumber2)+","+str(winnumber3)+","+str(winnumber4)+","+str(winnumber5)+","+str(winnumber6)+","+str(winnumber7)+","+str(sellprice)+","+str(wins1)+","+str(price1)+","+str(wins2)+","+str(price2)+","+str(wins3)+","+str(price3)+","+str(sunday)+","+str(sales)+","+str(poolmoney)+",'"+str(memo)+"')"
    try:
        mycursor.execute(sqlstr)
        conn.commit()
    except Exception:
        print('primary key error')
    mycursor.close()

def fill_dateid(dateid):
    """
        about abbreviation ssq period numebr fill complete
        if number is 4 char then fill 3 char
        if number is 5 char then fill 2 char
    """
    longs=len(dateid)
    if longs==4:
        return '200'+dateid
    elif longs==5:
        return '20'+dateid
    else:
        return dateid

def read_winnumberinfo_websocket(stcode,edcode):
    """
        http://www.cwl.gov.cn/cwl_admin/front/cwlkj/search/kjxx/findDrawNotice?name=ssq&issueCount=&issueStart=2021100&issueEnd=2022001&dayStart=&dayEnd=
        这个接口每次只返回100条记录，并且是根据期号倒序返回，需要注意。
    """
    r=requests.get("http://www.cwl.gov.cn/cwl_admin/front/cwlkj/search/kjxx/findDrawNotice?name=ssq&issueCount=&issueStart="+str(stcode)+"&issueEnd="+str(edcode)+"&dayStart=&dayEnd=")
    nums=r.text
    num_dict=json.loads(nums)
    return num_dict['result']

def import_socket(conns,resto):
    """
        code    期数
        date    开奖日期带星期
        week    星期几
        red     红色球，号分割6位
        blue    蓝色球
        sales   销售额
        poolmoney   流转下月奖池金额
        content 兑奖公告
    """
    for nr_list in resto:
        r=split_red(nr_list["red"])
        add_ssq(conns,nr_list["code"],split_date(nr_list["date"]),r[0],r[1],r[2],r[3],r[4],r[5],nr_list["blue"],nr_list["sales"],'null','null','null','null','null','null',return_weeknum(split_week(nr_list["date"])),nr_list["sales"],nr_list["poolmoney"],nr_list["content"])

def split_red(red):
    r=red.split(",")
    return r

def split_date(date_):
    return date_[:10]

def split_week(date_):
    return date_[11:12]

def return_weeknum(weeks):
    return weekdict[weeks]    

if __name__ == '__main__':
    conns=ssq_connect('johnny','Ly818379','localhost','ssq')
    # 针对2013年版本双色球excel数据表做的数据导入数据库的函数
    # import_excel(table,conns)

    #针对2021年版本福彩网往期开奖记录接口的导入数据，一次只能查询100期，注意
    res=read_winnumberinfo_websocket('2021100','2022001')
    import_socket(conns,res)
    conns.close()