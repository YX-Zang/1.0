import requests
import pandas as pd
import xml.etree.cElementTree as et
import database_settings
from sqlalchemy import create_engine
import urllib
import time
now = time.strftime('%Y-%m-%d',time.gmtime())
con = create_engine('mssql+pyodbc://'+database_settings.User+':'+database_settings.Password+'@'+database_settings.Server+':'+database_settings.Port+'/'+database_settings.Database+'?driver=ODBC Driver 13 for SQL Server')
data = []


def Crawl_XML(President_No,Agency):
    proxy = 'http://proxy.yahoo.com.tw:1234/'
    format = 'xml'
    Application_Code = '7E6AFA72-AD6A-46D3-8681-ED77951D912D'
    link = 'http://data.gcis.nat.gov.tw/od/data/api/' + Application_Code + '?$format=' + format + '&$filter=President_No eq ' + President_No + ' and Agency eq ' + Agency
    res_encode = proxy + urllib.parse.quote_plus(link)
    #proxy
    #res = requests.get(res_encode)
    # no proxy
    res = requests.get(link)
    tree = et.ElementTree(et.fromstring(res.text))
    root = tree.getroot()
    elem_list = []
    #print('root')
    #print(root.text)
    #print('tree')
    #print(tree)
    if (root.text != None):
        print('Do')
        for elements in range(0, 12):
            elem_list.append(root[0][elements].text)
        elem_list.insert(0,now)
        return elem_list
    else:
        print('None')




def SQL():
    list_city = []
    for i in con.execute("select replace(A.CustID,' ',''),replace(AgencyCode,' ',''),replace(Address,' ','') "
                         " from( "
                         " select ROW_NUMBER()over(partition by CustID order by CustID)rI, "
                         " CustID,Address = case when Address like '北市%' or Address like '%台北市%' or Address like '%臺北市%' then '臺北市' "
                         " when Address like '%北縣%' or Address like '%新北市%' then '新北市' "
                         " when Address like '高市%' or Address like '雄市%'    or Address like '%高雄市%'  then '高雄市' "
                         " when Address like '高縣%' or Address like '雄縣%'    or Address like '%高雄縣%'  then '高雄市' "
                         " when Address like '基市%' or Address like '隆市%'   or Address like '%基隆市%'  then '基隆市' "
                         " when Address like '竹市%' or Address like '%新竹市%'  or Address like '%新竹%' then '新竹市' "
                         " when Address like '竹縣%' or Address like '新縣%'    or Address like '%新竹縣%'  then '新竹縣' "
                         " when Address like '中市%' or Address like '%中縣%' or Address like '%台中%' or Address like '%臺中%'  or  Address like '臺中市%' then '臺中市' "
                         " when Address like '南市%' or Address like '%台南市%' or Address like '%南縣%' or Address like '%臺南%'  or Address like '臺南市%' then '臺南市' "
                         " when Address like '嘉市%' or Address like '嘉市%'    or Address like '%嘉義市%'  then '嘉義市' "
                         " when Address like '桃市%' or Address like '%桃園市%'  then '桃園市' "
                         " when Address like '桃縣%' or Address like '園縣%'    or Address like '%桃園縣%'  then '桃園縣' "
                         " when Address like '宜縣%' or Address like '蘭縣%'    or Address like '%宜蘭縣%'  then '宜蘭縣' "
                         " when Address like '苗縣%' or Address like '栗縣%'    or Address like '%苗栗縣%'  then '苗栗縣' "
                         " when Address like '彰縣%' or Address like '%彰化縣%'  then '彰化縣' "
                         " when Address like '%南投縣%'  then '南投縣' "
                         " when Address like '雲縣%' or Address like '%雲林縣%'  then '雲林縣' "
                         " when Address like '嘉縣%' or Address like '義縣%'    or Address like '%嘉義縣%'  then '嘉義縣' "
                         " when Address like '屏縣%' or Address like '%屏東縣%'  then '屏東縣' "
                         " when Address like '澎縣%' or Address like '%澎湖縣%'  then '澎湖縣' "
                         " when Address like '花縣%' or Address like '%花蓮縣%'  then '花蓮縣' "
                         " when Address like '%台東縣%' or Address like '%臺東縣%'  then '臺東縣' "
                         " when Address like '%金門%'   then '金門縣' "
                         " when Address like '%連江%'   then '連江縣' "
                         " else Address "
                         " end "
                         " from AMD..tblAllCustContact "
                         " where CustID in ( "
                         " select IDNOF "
                         " from [EMR].[dbo].[INVESTOR] "
                         "where I_CANCEL = '' and len (IDNOF) = 8"
                         " and IDNOF not in (select cast(營利事業統一編號 as varchar) from REG..tblCompanyInfo)) "
                         " and AccountClass = 001)A "
                         " left join REG..tblAgencyCode on A.Address = City "
                         " where A.rI = 1 and A.Address is not null and AgencyCode is not null and A.CustID ").fetchall():
                        #" where A.rI = 1 and A.Address is not null and AgencyCode is not null and A.CustID not like '%[a-zA-z]%' ").fetchall():
        list_city.append(i)
    for President_No, Agency in list_city:
        print('President_No = ' + President_No, 'AgencyCode =' + Agency)
        data.append(Crawl_XML(President_No, Agency))


def SQL2():
    AgencyCode_list = []
    for i in con.execute('SELECT [Business_Account_No],[Agency] FROM [dbo].[Business_list]').fetchall():
        AgencyCode_list.append(i)
    #list_a = [('82118175','376410000A'),('82118512','376410000A'),('08548967','376410000A'),('97172936','376410000A')]
    for President_No, Agency in AgencyCode_list:
        print('a=' + President_No, 'b=' + Agency)
        data.append(Crawl_XML(President_No, Agency))



def insert():
    Table = 'tblAgencyCode3'
    print('Start Write Data to DB')
    data2 = [x for x in data if x is not None]
    con = create_engine(
        'mssql+pyodbc://' + database_settings.User + ':' + database_settings.Password + '@' + database_settings.Server + ':' + database_settings.Port + '/' + database_settings.Database + '?driver=ODBC Driver 13 for SQL Server')
    data2 = pd.DataFrame(data2,columns=['更新日期','登記機關','登記機關名稱','商業統一編號','商業名稱','商業登記地址','資本額','組織型態','組織型態名稱','代表人姓名','核准設立日期','變更日期','商業狀態'])
    data2.to_sql(Table, con, if_exists='append', index=False)
    print('Write Data Done')
    print('------Crawler Work Finish------')



if __name__ == '__main__':
    SQL2()
    #消除list中的None
    #data = [x for x in data if x is not None]
    #print(data)
    df_data = pd.DataFrame(data,
                           columns=['更新日期','登記機關', '登記機關名稱', '商業統一編號', '商業名稱', '商業登記地址', '資本額', '組織型態', '組織型態名稱', '代表人姓名',
                                    '核准設立日期', '變更日期', '商業狀態'])
    print(df_data)
    #insert()

