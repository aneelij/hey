#!/home/pso/mytest/bin/python
# -*- coding: utf-8 -*-
import sys
import os,os.path
import commands
import zipfile
import smtplib
import email.MIMEMultipart# import MIMEMultipart
import email.MIMEText# import MIMEText
import email.MIMEBase# import MIMEBase
import sys
reload(sys)
sys.setdefaultencoding('utf8')

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import pprint
import getopt
import time
import datetime
import cx_Oracle
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from pyh import *
from sys import argv
import pymysql
import MySQLdb

SQL_ORDERED_BY_ELAPSED_TIME = """SELECT  conv( a.checksum, 10, 16 ),ROUND(sum(a.Query_time_sum),4),sum(a.ts_cnt),ROUND(max(a.Query_time_pct_95),4),max(a.sample) 
FROM slow_query_log.global_query_review_history a WHERE a.User_max = %(dbname)s AND a.ts_max > TIMESTAMP ( adddate(date(sysdate()), - 7)) 
AND a.ts_min < TIMESTAMP (adddate(date(sysdate()), 0)) AND HOUR(a.ts_max) >= 7 
AND HOUR(a.ts_max) <= 19  and WEEKDAY(a.ts_max) >=0 and WEEKDAY(a.ts_max) <6 
and date_format(a.ts_min, '%%Y-%%m-%%d %%H')=%(snap_id)s
group by conv( a.checksum, 10, 16 ) order by sum(a.Query_time_sum) desc limit 10
"""


def query_ora_obj_size_by_num(cursor, dbname):
    cursor.execute(
        "SELECT table_name,table_rows,table_type,concat(round(data_length / (1024 * 1024), 2), 'M') data_length,concat(round(index_length / (1024 * 1024), 2), 'M') index_length,concat(round(round(data_length + index_length) / (1024 * 1024), 2), 'M') total_size FROM information_schema.TABLES where (ENGINE = 'MyISAM' or ENGINE = 'InnoDB') and table_schema = '"+ dbname +"' ORDER BY (data_length + index_length) DESC limit 10")
    records = cursor.fetchall()
    return records


def query_ora_obj_by_rows(cursor, p_schema_name, p_num):
    cursor.execute("select * from (select table_name,num_rows,to_char(last_analyzed,'yyyy-mm-dd hh24:mi:ss') from dba_tables where owner='" + p_schema_name + "' and num_rows is not null order by 2 desc) where rownum<=" + p_num)
    records = cursor.fetchall()
    return records


def query_sql_text( mysqlcursor, p_sql_id):
    mysqlcursor.execute("select  a.sql_text from db_sql_alert_sqltext a where a.sql_id='" + p_sql_id + "'")
    records = mysqlcursor.fetchall()
    if not records or records[0][0].strip() == '':
        return u'sql文本没有找到'
    else:
        return records[0][0]


def query_db_id(cursor):
    cursor.execute("select dbid from v$database")
    records = cursor.fetchall()
    return records[0][0]

def query_sql_plan(cursor, p_sql_id, p_begin_snap_id, p_end_snap_id,schema):
    cursor.execute("select distinct a.plan_hash_value from dba_hist_sqlstat a, dba_hist_snapshot b where sql_id = '" + p_sql_id + "' and a.snap_id = b.snap_id and a.snap_id between " + p_begin_snap_id + " and " + p_end_snap_id + " and parsing_schema_name='" +schema +"'")
    records = cursor.fetchall()
    return records


def query_plan_data(cursor, p_sql_text):
    cursor.execute("explain "+p_sql_text+"")
    records = cursor.fetchall()
    return records


def query_snapid(cursor, schema):
    cursor.execute("""SELECT  date_format(a.ts_min, '%Y-%m-%d %H'),sum(a.Query_time_sum) sum_time FROM slow_query_log.global_query_review_history a WHERE a.Db_max = '"""+ schema +"""' AND a.ts_max > TIMESTAMP ( adddate(date(sysdate()), - 7)) AND a.ts_min < TIMESTAMP (adddate(date(sysdate()), 0)) AND HOUR(a.ts_max) >= 7 AND HOUR(a.ts_max) <= 19  and WEEKDAY(a.ts_max) >=0 and WEEKDAY(a.ts_max) <6  group by date_format(a.ts_min, '%Y-%m-%d %H') ORDER BY sum_time desc limit 1""")
    records = cursor.fetchall()
    return records[0][0]


def query_schema_exist(p_dbinfo, p_schema_name):
    conn = cx_Oracle.connect(p_dbinfo[3] + '/' + p_dbinfo[4] + '@' + p_dbinfo[0] + ':' + p_dbinfo[1] + '/' + p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select count(*) from dba_users where username='" + p_schema_name + "'")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    if int(records[0][0]) == 1:
        return True
    else:
        return False


def query_end_snap_id(p_dbinfo, p_reportdate, p_hour):
    conn = cx_Oracle.connect(p_dbinfo[3] + '/' + p_dbinfo[4] + '@' + p_dbinfo[0] + ':' + p_dbinfo[1] + '/' + p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select min(snap_id) from wrm$_snapshot where end_interval_time >= to_date('" + p_reportdate + " " + p_hour + "','yyyy-mm-dd hh24') and instance_number=1")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records[0][0]


def query_snap_data(cursor, p_begin_snap_id, p_end_snap_id):
    cursor.execute("select snap_id,to_char(begin_interval_time, 'yyyy-mm-dd hh24:mi:ss'),to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss'),trunc(begin_interval_time) from dba_hist_snapshot where snap_id between " + p_begin_snap_id + " and " + p_end_snap_id + " and rownum<=1 ")
    records = cursor.fetchall()
    return records


def query_sql_data(p_dbinfo, p_sql):
    conn = cx_Oracle.connect(p_dbinfo[3] + '/' + p_dbinfo[4] + '@' + p_dbinfo[0] + ':' + p_dbinfo[1] + '/' + p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute(p_sql)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records


def print_html_header(p_schema_name):
    page = PyH(p_schema_name + '_SQL Report')
    page << """<meta charset="utf-8">"""
    page << """<style type="text/css">
            body.awr {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black;}
            pre.awr  {font:10pt Courier;color:black; background:White;}
            h1.awr   {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#336699;border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;}
            h2.awr   {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h3.awr   {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h4.awr   {font:bold 14pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h5.awr   {font:bold 12pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h6.awr   {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            li.awr   {font: 10pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;}
            th.awrnobg  {font:bold 10pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;padding-left:4px; padding-right:4px;padding-bottom:2px}
            td.awrbg    {font:bold 10pt Arial,Helvetica,Geneva,sans-serif; color:White; background:#0066CC;padding-left:4px; padding-right:4px;padding-bottom:2px}
            td.awrnc    {font:10pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;}
            td.awrc     {font:10pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;}
            a.awr       {font:bold 10pt Arial,Helvetica,sans-serif;color:#663300; vertical-align:top;margin-top:0pt; margin-bottom:0pt;}
            </style>"""
    page << """<SCRIPT>
            function isHidden(oDiv,oTab){
              var vDiv = document.getElementById(oDiv);
              var vTab = document.getElementById(oTab);
              vDiv.innerHTML=(vTab.style.display == 'none')?"<h5 class='awr'>-</h5>":"<h5 class='awr'>+</h5>";
              vTab.style.display = (vTab.style.display == 'none')?'table':'none';
            }
            </SCRIPT>"""
    page << """<head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            </head>"""
    page << h3(u'阅读须知:')
    page << h3(u'1. 本周工作日早7点至晚7点中负载最大时段(一个小时)提取的TOP 10 sql ')
    page << h3(u'2. 告警sql为本周抓取的一些异常等待事件、执行时间长的sql，如行锁等待')
    page << h3(u'3. Mysql中运行超过0.1s的sql为慢sql')
    page << h3(u'4. 有问题请及时联系数据库组，或者发邮件给"IT.Dba.list@CREDITEASE.CN"')

    page << h1(p_schema_name + u' 数据库报告', cl='awr')
    return page


def print_db_header(p_page, p_ip, p_schema_name, p_begin_hour, p_end_hour):
    p_page << br()
    l_header = [u'数据库 IP', u'数据库名',  u'开始时间', u'结束时间']
    l_data = [p_ip, p_schema_name, p_begin_hour, p_end_hour]

    mytab = p_page << table(border='1', width=1000)
    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class'] = 'awrbg'
        td_tmp.attributes['align'] = 'center'

    tabtr = mytab << tr()
    for o in l_data:
        td_tmp = tabtr << td(o)
        td_tmp.attributes['class'] = 'awrc'
        td_tmp.attributes['align'] = 'center'


def print_html_snap_tab(p_page, p_snap_info):
    p_page << br()
    l_header = ['SNAP_ID', 'BEGIN_TIME', 'END_TIME']
    l_data = p_snap_info

    p_page << h3('ORACLE SNAPSHOT INFORMATION', cl='awr')
    mytab = p_page << table(border='1', width=400)
    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class'] = 'awrbg'
        td_tmp.attributes['align'] = 'center'

    for j in range(0, len(l_data)):
        tabtr = mytab << tr()
        for i in range(0, len(l_data[j])):
            td_tmp = tabtr << td(l_data[j][i])
            td_tmp.attributes['class'] = 'awrc'
            td_tmp.attributes['align'] = 'center'
            if j % 2 == 0:
                td_tmp.attributes['class'] = 'awrc'
            else:
                td_tmp.attributes['class'] = 'awrnc'

    p_page << br()


def print_html_sql_tab(p_page, p_sqldata, p_sqltype, p_header):
    l_page = p_page
    l_data = p_sqldata
    l_header = p_header
    l_page << h3(p_sqltype, cl='awr')

    mytab = l_page << table(border='1', width=2000)
    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class'] = 'awrbg'
        td_tmp.attributes['align'] = 'center'

    for j in range(0, len(l_data)):
        tabtr = mytab << tr()
        for i in range(0, len(l_data[j])):

            if i == 0:  # sql_id
                td_tmp = tabtr << td()
                td_tmp.attributes['class'] = 'awrc'
                a_tmp = td_tmp << a(l_data[j][i])
                a_tmp.attributes['class'] = 'awr'
                a_tmp.attributes['href'] = '#' + l_data[j][i]
            else:
                if l_data[j][i]:
                    td_tmp = tabtr << td(l_data[j][i])
                else:
                    td_tmp = tabtr << td('0')

            if j % 2 == 0:
                td_tmp.attributes['class'] = 'awrc'
            else:
                td_tmp.attributes['class'] = 'awrnc'
            if i == (len(l_data[j]) - 1):
                td_tmp.attributes['align'] = 'left'
            else:
                td_tmp.attributes['align'] = 'right'

    l_page << br()


def print_html_ora_obj_size_tab(p_page, p_data):
    l_page = p_page
    l_data = p_data
    l_header = [u'对象名字', u'表行数', u'对象类型', u'表大小', u'索引大小', u'表加索引大小']

    l_page << h3(u'对象信息', cl='awr')

    mytab = l_page << table(border='1', width=800)
    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class'] = 'awrbg'
        td_tmp.attributes['align'] = 'center'

    for o in l_data:
        tabtr = mytab << tr()
        for i in range(0, len(o)):
            td_tmp = tabtr << td(o[i])
            td_tmp.attributes['class'] = 'awrc'
            if i == 0 or i == 1 or i == 2:
                td_tmp.attributes['align'] = 'left'
            else:
                td_tmp.attributes['align'] = 'right'
    p_page << br()


def print_html_ora_obj_rows_tab(p_page, p_data):
    l_page = p_page
    l_data = p_data
    l_header = [u'表名', u'表行数', u'统计信息更新时间']

    l_page << h3(u'表行数统计', cl='awr')

    mytab = l_page << table(border='1', width=800)
    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class'] = 'awrbg'
        td_tmp.attributes['align'] = 'center'

    for o in l_data:
        tabtr = mytab << tr()
        for i in range(0, len(o)):
            td_tmp = tabtr << td(o[i])
            td_tmp.attributes['class'] = 'awrc'
            if i == 0:
                td_tmp.attributes['align'] = 'left'
            else:
                td_tmp.attributes['align'] = 'right'
    p_page << br()


def print_html_sql_header(p_page, p_sql_id, p_sql_text):
    p_page << h4('sql id : ' + p_sql_id, cl='awr')
    l_header = "SQL Text"
    #l_data = p_sql_text.encode("utf-8")
    l_data = p_sql_text
    mytab = p_page << table(border='1', width=1200)

    headtr = mytab << tr()
    td_tmp = headtr << td(l_header)
    td_tmp.attributes['class'] = 'awrbg'
    td_tmp.attributes['align'] = 'center'

    a_tmp = td_tmp << a()
    a_tmp.attributes['class'] = 'awrc'
    a_tmp.attributes['name'] = p_sql_id

    tabtr = mytab << tr()
    #print l_data.encode("utf-8")
    td_tmp = tabtr << td(l_data)
    td_tmp.attributes['class'] = 'awrc'
    td_tmp.attributes['align'] = 'left'

    p_page << br()


def print_html_sql_exec(p_page, p_sql_exec):
    l_page = p_page
    l_data = p_sql_exec
    l_header = ['INSTANCE_NUMBER', 'SNAP_ID', 'PLAN_HASH_VALUE', 'BEGIN_INTERVAL_TIME']

    mytab = l_page << table(border='1', width=800)
    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class'] = 'awrbg'
        td_tmp.attributes['align'] = 'center'

    for j in range(0, len(l_data)):
        tabtr = mytab << tr()
        for i in range(0, len(l_data[j])):
            td_tmp = tabtr << td(l_data[j][i])
            td_tmp.attributes['class'] = 'awrc'
            td_tmp.attributes['align'] = 'right'
            if j % 2 == 0:
                td_tmp.attributes['class'] = 'awrc'
            else:
                td_tmp.attributes['class'] = 'awrnc'
    l_page << br()


def print_html_sql_plan(p_page, p_sql_id, p_sql_plan_hash_value, p_sql_plan_data, p_output_type):
    l_page = p_page
    l_data = p_sql_plan_data
    l_header = ['Id', 'select_type', 'table', 'type', 'possible_keys', 'key', 'key_len', 'ref', 'rows', 'Extra']

    mytab = l_page << table(border='0')
    headtr = mytab << tr()
    td_tmp = headtr << td()
    td_tmp << h5(u'执行计划 : ' + p_sql_plan_hash_value, cl='awr')
    td_tmp = headtr << td()
    div_tmp = td_tmp << div(id='div_' + p_sql_id + '_' + p_sql_plan_hash_value, style='cursor:hand', onclick="isHidden('" + 'div_' + p_sql_id + '_' + p_sql_plan_hash_value + "','tab_" + p_sql_id + '_' + p_sql_plan_hash_value + "')")
    if p_output_type == 'FILE':
        div_tmp << h5('+', cl='awr')
        mytab = l_page << table(id='tab_' + p_sql_id + '_' + p_sql_plan_hash_value, border='1', style="display:none")
    else:
        mytab = l_page << table(id='tab_' + p_sql_id + '_' + p_sql_plan_hash_value, border='1', style="display:table")

    headtr = mytab << tr()
    for i in range(0, len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class'] = 'awrbg'
        td_tmp.attributes['align'] = 'center'

    for j in range(0, len(l_data)):
        tabtr = mytab << tr()
        for i in range(0, len(l_data[j])):
            if l_data[j][i]:
                td_tmp = tabtr << td(str(l_data[j][i]).replace(' ', '&nbsp;'))
            else:
                td_tmp = tabtr << td()
            td_tmp.attributes['class'] = 'awrc'
            td_tmp.attributes['align'] = 'left'
            if j % 2 == 0:
                td_tmp.attributes['class'] = 'awrc'
            else:
                td_tmp.attributes['class'] = 'awrnc'
    l_page << br()


def send_rpt_mail(p_page, p_rpt_emails, p_report_date, p_report_dbinfo):
    html_tmpfile = '/tmp/html_tmpfile'
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = 'Report Database Report_' + p_report_dbinfo[0] + ' (' + p_report_date + ')'
    p_page.printOut(file=html_tmpfile)
    fo = open(html_tmpfile)
    htmltext = fo.read()
    fo.close()
    msgText = MIMEText(htmltext, 'html')
    msgRoot.attach(msgText)
    smtp = smtplib.SMTP()
    smtp.connect('105.43.123.5)
    smtp.login("DBA@CREDITEASE.CN", "F34d2df$#@34")
    for mail_address in p_rpt_emails:
        smtp.sendmail("DBA@CREDITEASE.CN", mail_address, msgRoot.as_string())
    smtp.quit()



def get_dbant_dbinfo(cursor):
    dbinfo_sql = """SELECT a.db_name,a.id, b.db_vip,c.app_name,c.email, a.db_inst_id,b.db_type,a.line_username,b.port FROM db_sql_report_info c INNER JOIN  db_config a INNER JOIN db_inst b WHERE c.db_name = a.db_name and a.db_inst_id = b.id AND a.line_password IS NOT NULL AND a.line_password <> '' AND a.db_name in ('ffa') and b.db_type='mysql' """
    cursor.execute(dbinfo_sql)
    results = cursor.fetchall()
    return results



def get_operation_result(db_id ,sql_id, cursor):
    dbname_arg = {}
    dbname_arg.update({"db_id": db_id})
    dbname_arg.update({"sql_id": sql_id})
    operation_result = """select 
CASE WHEN a.app_status=1 THEN '正常' when a.app_status=2 THEN '忽略' when a.app_status=3 THEN '处理中' when a.app_status=4 THEN '处理完' ELSE a.app_status END,
CASE WHEN a.dba_status=1 THEN '正常' when a.dba_status=2 THEN '忽略' when a.dba_status=3 THEN '需处理' when a.app_status=4 THEN '处理完' ELSE a.dba_status END,
a.contact,
a.expect_time from db_sql_alert a where a.db_id=%(db_id)s and a.sql_id=%(sql_id)s """
    cursor.execute(operation_result, dbname_arg)
    results = cursor.fetchall()
    data = []
    if results[0][0] == None:
        data.append(u'无')
    else:
        data.append(results[0][0])
    if results[0][1] == None:
        data.append(u'无')
    else:
        data.append(results[0][1])
    if results[0][2] == None:
        data.append(u'无')
    else:
        data.append(results[0][2])
    if results[0][3] == None:
        data.append(u'无')
    else:
        data.append(str(results[0][3]))
    return data


def insert_alert_sql(db_id,sql_id, db_inst_id,cursor,conn,sqltext,sqlcount,sqltime):
    sqlalert_count = """select count(*) from db_sql_alert a where a.db_id=%(db_id)s and a.sql_id=%(sql_id)s limit 1"""
    dbname_arg = {"db_id":db_id,"sql_id":sql_id}
    cursor.execute(sqlalert_count, dbname_arg)
    results = cursor.fetchall()
    print db_id,results[0][0],sql_id
    if results[0][0] == 0:
        print "insert insert_alert_sql"
        alert_arg = {"db_id": db_id, "sql_id": sql_id,"db_inst_id":db_inst_id,"sqlcount":sqlcount,"sqltime":sqltime}
        alertlog_arg = {"sql_id": sql_id}
        alertsqltext_arg = {"sql_id": sql_id, "sqltext": sqltext}
        alertsqltext = "insert into db_sql_alert_sqltext(sql_id,sql_text,create_time) values(%(sql_id)s,%(sqltext)s,now())"
        alertlog = "insert into db_sql_alert_log(sql_id,status,operator_time) values(%(sql_id)s,'1',now())"
        alertsql = "insert into db_sql_alert(sql_id,db_id,db_inst_id,status,create_time,event_name,max_cnt,max_timewaited) values(%(sql_id)s,%(db_id)s,%(db_inst_id)s,'1',now(),'zhoubao',%(sqlcount)s,%(sqltime)s)"
        h = cursor.execute(alertsql,alert_arg)
        h = cursor.execute(alertlog,alertlog_arg)
        h = cursor.execute(alertsqltext,alertsqltext_arg)
        conn.commit()
    return results[0][0]


def query_alert_sql(db_id, cursor):
    dbid_arg = {}
    dbid_arg.update({"db_id":db_id})
    alert_sql = """SELECT b.sql_id, CASE WHEN e.event_comment IS NULL THEN b.event_name ELSE e.event_comment END, b.create_time, b.max_timewaited, b.count,
    CASE WHEN b.app_status=1 THEN '正常' when b.app_status=2 THEN '忽略' when b.app_status=3 THEN '处理中' when b.app_status=4 THEN '处理完' ELSE '无' END,
    CASE WHEN b.dba_status=1 THEN '正常' when b.dba_status=2 THEN '忽略' when b.dba_status=3 THEN '需处理' when b.app_status=4 THEN '处理完' ELSE '无' END,
        CASE WHEN b.contact IS NULL THEN '无' ELSE b.contact END,
        CASE WHEN b.expect_time IS NULL THEN '无' ELSE b.expect_time END
        FROM db_sql_alert b LEFT JOIN event_comment e on b.event_name=e.event_name where  b.event_name not in ('zhoubao1') and db_id=%(db_id)s and b.create_time >timestamp(adddate(date(sysdate()), -7))  and b.create_time <timestamp(adddate(date(sysdate()), 0)) order by b.count desc,b.create_time"""
    cursor.execute(alert_sql, dbid_arg)
    results = cursor.fetchall()
    results = [(value[0], value[1], str(value[2]), str(value[3]), value[4],str(value[5]),value[6],value[7],str(value[8])) for value in results]
    return tuple(results)

def get_filename(filename,base_path):
    full_filename = base_path + filename
    zip_full_filename = full_filename+'.zip'
    filesize = os.path.getsize(full_filename)
    if filesize >= 2097152:
        zf = zipfile.ZipFile(zip_full_filename, "w", zipfile.zlib.DEFLATED)
        zf.write(full_filename,filename)
        zf.close()
        return zip_full_filename
    return full_filename

def send_zhoubao_mail(filename,dstmail,schema):
    From = "DBA@CREDITEASE.CN"
    To = dstmail
    file_name1 = filename
    file_name2 = r"/home/pso/awr_schema/report/数据库周报阅读方法.pdf".decode("utf-8")
    file_name3 = r"/home/pso/awr_schema/report/数据库周报反馈表.xlsx".decode("utf-8")
    server = smtplib.SMTP("10.160.194.8")
    server.login("DBA@CREDITEASE.CN", "4Jnv*ck3#ckeH")
    main_msg = email.MIMEMultipart.MIMEMultipart()
    begindate = datetime.date.today() + datetime.timedelta(days=-7)
    enddate = datetime.date.today() + datetime.timedelta(days=-1)
    text3 = u"3.数据库周报时间范围: " + str(begindate) + u"~" + str(enddate)
    text_msg = email.MIMEText.MIMEText(u"Hi\n\t1.重点关注部分包括\"SQL 执行时间 TOP 10\" 和\"告警SQL\"\n\t2.研发人员需要填写数据库周报反馈表\n\t"+text3+"\n\t4.有问题请及时联系数据库组，或者发邮件给\"IT.Dba.list@CREDITEASE.CN\"\n", _charset="utf-8")
    main_msg.attach(text_msg)
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)
    data1 = open(file_name1, 'rb')
    data2 = open(file_name2, 'rb')
    data3 = open(file_name3, 'rb')
    file_msg1 = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg2 = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg3 = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg1.set_payload(data1.read())
    file_msg2.set_payload(data2.read())
    file_msg3.set_payload(data3.read())
    data1.close()
    data2.close()
    data3.close()
    email.Encoders.encode_base64(file_msg1)
    email.Encoders.encode_base64(file_msg2)
    email.Encoders.encode_base64(file_msg3)
    basename1 = os.path.basename(file_name1)
    basename2 = os.path.basename(file_name2)
    basename3 = os.path.basename(file_name3)
    file_msg1.add_header('Content-Disposition', 'attachment', filename=basename1)
    file_msg2.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', basename2.encode("utf-8")))
    file_msg3.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', basename3.encode("utf-8")))
    main_msg.attach(file_msg1)
    main_msg.attach(file_msg2)
    main_msg.attach(file_msg3)
    main_msg['From'] = From
    main_msg['To'] = ",".join(To)
    if schema:
        main_msg['Subject'] = schema + u"数据库周报"
    else:
        main_msg['Subject'] = u"数据库周报"
    main_msg['Date'] = email.Utils.formatdate()
    fullText = main_msg.as_string()
    server = smtplib.SMTP("10.160.194.8")
    server.login("DBA@CREDITEASE.CN", "4Jnv*ck3#ckeH")
    server.sendmail(From, To, fullText)

def get_db_email(dbname,dbtype, cursor):
    dbname_arg = {}
    dbname_arg.update({"dbname": dbname})
    dbname_arg.update({"dbtype": dbtype})
    email_sql = """select email from db_sql_report_info   where db_name=%(dbname)s and db_type=%(dbtype)s"""
    cursor.execute(email_sql, dbname_arg)
    results = cursor.fetchall()
    email = results[0][0]
    email = email.replace(' ', '')
    email_list = email.strip().split(';')
    return email_list

def get_zip_filename(base_path, today):
    filelist = os.listdir(base_path)
    filelistname = [value for value in filelist if 'zip' not in value]
    full_filename_zip = base_path+'database_zhoubao_'+ today +'.zip'
    print full_filename_zip
    zf = zipfile.ZipFile(full_filename_zip, "w", zipfile.zlib.DEFLATED)
    for value in filelistname:
        zf.write(base_path+value, value)
    zf.close()
    os.system('ls -lrt ' + full_filename_zip)
    return full_filename_zip

def get_db_config_by_dbid(cursor, db_id):
    sql = "select c.db_name,s.db_type,s.db_purpose,s_dev.ip dev_ip,s_dev.port dev_port,s.ip,s.port," \
          "c.db_service_name,c.line_username,from_base64(c.line_password),c.id from db_config c " \
          "inner join db_inst s on c.db_inst_id=s.id left join db_inst_dev dev_inst on s.id=dev_inst.db_inst_id " \
          "left join db_inst s_dev on s_dev.id=dev_inst.db_inst_dev_id where c.id = %(sdb_id)s"
    cursor.execute(sql, {"db_id": db_id})
    result = cursor.fetchall()
    return result


if __name__ == "__main__":
    today = datetime.date.today().strftime('%Y%m%d')
    v_output_type = "FILE"
    base_path = r"/home/pso/awr_schema/mysql/" + today + "/"
    os.system('mkdir -p '+base_path)
    dbant_dbinfo = ['10.130.32.21' ,'3306' ,'db_backup', '4J87A3G9ozz4', 'dbant']
    mysqlconn = MySQLdb.connect(host=dbant_dbinfo[0], port=int(dbant_dbinfo[1]), user=dbant_dbinfo[2], passwd=dbant_dbinfo[3], db=dbant_dbinfo[4], charset="utf8", connect_timeout=10)
    mysqlcursor = mysqlconn.cursor()
    zhoubao_dbinfo = get_dbant_dbinfo(mysqlcursor)
    print zhoubao_dbinfo
    for dbinfo in zhoubao_dbinfo:
        v_tuning_sql = []  # tuning sql list
        user_ip = dbinfo[2]
        dbname = dbinfo[0].lower()
        username = dbinfo[7].lower()
        db_id = dbinfo[1]
        app_name = dbinfo[3]
        dbtype = dbinfo[6]
        app_email = get_db_email(dbname,dbtype,mysqlcursor)
        app_dbinst = dbinfo[5]
        print user_ip,dbname,username,db_id,app_name.decode('utf-8'),app_email
        port = dbinfo[8]
        userconn = MySQLdb.connect(host=user_ip, port=int(port), user=dbant_dbinfo[2], passwd=dbant_dbinfo[3], db=dbname, charset="utf8", connect_timeout=10)
        usercursor = userconn.cursor()
        usercursor.execute("select database()")
        test2 = usercursor.fetchall()
        print test2[0][0]
        snap_id = query_snapid(usercursor, dbname)
        print snap_id
        condition_arg = {}
        condition_arg.update({"dbname": dbname, "snap_id": snap_id})
        print condition_arg
        usercursor.execute(SQL_ORDERED_BY_ELAPSED_TIME, condition_arg)
        v_sqldata = usercursor.fetchall()
        for o in v_sqldata:
            if o[0] not in v_tuning_sql:
                v_tuning_sql.append(o[0])
                insert_alert_sql(db_id, o[0], app_dbinst, mysqlcursor, mysqlconn,o[4], o[2],o[3])
        print v_sqldata[0]
        print_v_sqldata = [[str(value[0]),value[1],value[2],value[3],value[4][:30]] for value in v_sqldata]
        v_sqldata_new = []
        for value in print_v_sqldata:
            sql_id = list(value)[0]
            list_sqldata = list(value)
            adddata = get_operation_result(db_id, sql_id, mysqlcursor)
            list_sqldata.append(adddata[0])
            list_sqldata.append(adddata[1])
            list_sqldata.append(adddata[2])
            list_sqldata.append(adddata[3])
            v_sqldata_new.append(list_sqldata)
        v_objdata_size = query_ora_obj_size_by_num(usercursor, dbname)
        snap_datainfo = [snap_id+':00',snap_id+':59']
        v_page = print_html_header(dbname)
        print_db_header(v_page, user_ip, dbname, snap_datainfo[0], snap_datainfo[1])
        v_page << br()
        print_html_ora_obj_size_tab(v_page, v_objdata_size)
        v_header = ['SQL Id', u'执行总时间(s)', u'慢查询执行总次数', u'平均每次执行时间(s)', u'SQL文本', u'研发反馈', u'dba反馈', u'负责人', u'预计完成时间']
        print_html_sql_tab(v_page ,v_sqldata_new ,u'SQL 执行时间 TOP 10' ,v_header)
        v_sqlalertdata = query_alert_sql(db_id, mysqlcursor)
        v_sqlalert_header = ['SQL Id', u'等待事件', u'本周首次发现时间', u'执行或者等待时间(s)', u'告警次数', u'app状态', u'dba状态', u'负责人', u'预计完成时间']
        if  v_sqlalertdata:
            print_html_sql_tab(v_page ,v_sqlalertdata ,u'告警SQL' ,v_sqlalert_header)
            for o in v_sqlalertdata:
                if o[0] not in v_tuning_sql:
                    v_tuning_sql.append(o[0])
        v_page << br()
        v_page << h1('', cl='awr')
        for o in v_tuning_sql:
            v_sql_id = o
            v_sql_text = query_sql_text( mysqlcursor ,v_sql_id)
            print_html_sql_header(v_page ,v_sql_id ,v_sql_text)
            v_sql_plan = [' ']  # hash value
            v_sql_plan_hash_value = ' '
            v_sql_plan_data = query_plan_data(usercursor, v_sql_text)
            print_html_sql_plan(v_page ,v_sql_id ,v_sql_plan_hash_value ,v_sql_plan_data ,v_output_type)
            v_page << br()
            v_page << h1('', cl='awr')
        usercursor.close()
        userconn.close()
        filename = dbname + '_'+ today + '.html'
        schema = app_name.encode('utf-8')+dbname
        v_page.printOut(file=base_path+filename)
        get_new_filename = get_filename(filename,base_path)
        # print get_new_filename
        # print app_email
        # # app_email.append('IT.Dba.list@CREDITEASE.CN')
        # # app_email.append('songliang@creditease.cn')
        # # emaillist.append('arch.review.list@CREDITEASE.CN')
        test_email_list=['youyuan2@creditease.cn','youyuan2@creditease.cn']
        send_zhoubao_mail(get_new_filename,test_email_list,schema)
    mysqlcursor.close()
    mysqlconn.close()
    # #zip_mail = ['arch.review.list@CREDITEASE.CN']
    # zip_mail = ['youyuan2@CREDITEASE.CN','youyuan2@creditease.cn']
    # #zip_mail.append('IT.Dba.list@CREDITEASE.CN')
    # full_filename_zip = get_zip_filename(base_path, today)
    # send_zhoubao_mail(full_filename_zip, zip_mail, None)
