import xlrd
from xlrd import xldate_as_tuple
import datetime
import mysql.connector as connt

data1=xlrd.open_workbook("/Users/liangye/Downloads/ssq.xls")
table=data1.sheets()[0]

tables=[]

def import_excel(excel,conn):
    countflag='2013066'
    for rown in range(excel.nrows):
        dateidstr=""
        try:
            dateidstr=fill_dateid(str(int(table.cell_value(rown,1))))
            add_ssq(conn,dateidstr,'null',table.cell_value(rown,9),table.cell_value(rown,10),table.cell_value(rown,11),table.cell_value(rown,12),table.cell_value(rown,13),table.cell_value(rown,14),int(table.cell_value(rown,15)),'null','null','null','null','null','null','null',False)
        except Exception:
            print('转换内容失败！不导入数据库')
        if countflag==dateidstr:
            break
    
def ssq_connect(user,pwd,host,database):
    con=None
    try:
        con=connt.connect(user=user,password=pwd,host=host,database=database)
    except Exception:
        print('数据库连接失败!')
        exit(1)
    else:
        return con

def add_ssq(conn,dateid,windate,winnumber1,winnumber2,winnumber3,winnumber4,winnumber5,winnumber6,winnumber7,sellprice,wins1,price1,wins2,price2,wins3,price3,sunday):
    mycursor=conn.cursor()

    sqlstr="insert into number_info(dateid, windate, winnumber1, winnumber2, winnumber3, winnumber4, winnumber5, winnumber6, winnumber7, sellprice, wins1, price1, wins2, price2, wins3, price3, sunday) values ("+str(dateid)+","+str(windate)+","+str(winnumber1)+","+str(winnumber2)+","+str(winnumber3)+","+str(winnumber4)+","+str(winnumber5)+","+str(winnumber6)+","+str(winnumber7)+","+str(sellprice)+","+str(wins1)+","+str(price1)+","+str(wins2)+","+str(price2)+","+str(wins3)+","+str(price3)+","+str(sunday)+")"
    mycursor.execute(sqlstr)
    conn.commit()
    mycursor.close()

def fill_dateid(dateid):
    longs=len(dateid)
    if longs==4:
        return '200'+dateid
    elif longs==5:
        return '20'+dateid
    else:
        return dateid

if __name__ == '__main__':
    conns=ssq_connect('johnny','Ly818379','localhost','ssq')
    import_excel(table,conns)
    conns.close()