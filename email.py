# -*- coding=utf-8 -*-
import urllib2,urllib
import json
import traceback
from email.mime.multipart import MIMEMultipart
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
# update
def send(apiUrl,data):
    data_json = json.dumps(data)
    headers = {'Content-Type': 'application/json'} # 设置数据为json格式，很重要
    request = urllib2.Request(url=apiUrl,data=data_json)# headers=headers, 
    response = urllib2.urlopen(request)
    print response.read()

    content = json.dumps(response.read())#_,encoding="UTF-8", ensure_ascii=False)
    result = {'code':response.getcode(),'content':content}
    print("调用[%s]返回结果:%r" % (apiUrl,result))
    return result

#coding=utf-8
import smtplib
from email.mime.text import MIMEText

def send_email():
    msg_from='123123@qq.com'
    passwd='asdfewrewtee'
    msg_to='12312323@qq.com'

    
                                
    subject="python邮件测试"#主题     
    content="这是我使用python smtplib及email模块发送的邮件"#正文
    #msg = MIMEText(content)
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to

    #jpg类型附件
    part = MIMEApplication(open('out.jpg','rb').read())
    part.add_header('Content-Disposition', 'attachment', filename="qr.jpg")
    msg.attach(part)

    try:
        s = smtplib.SMTP_SSL("smtp.qq.com",465)#邮件服务器及端口号
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print "发送成功"
    except Exception as e:
        print e
        print "发送失败"
        print e.message
        print str(e).decode('UTF-8')
        print (unicode(str(e),encoding="utf-8"))

def email(server,port,msg_from,passwd,msg_to,subject,content,attatch_file):

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to

    #jpg类型附件
    part = MIMEApplication(open(attatch_file,'rb').read())
    part.add_header('Content-Disposition', 'attachment', filename=attatch_file)
    msg.attach(part)

    try:
        s = smtplib.SMTP(server,port)#邮件服务器及端口号
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print "发送成功"
    except Exception as e:
        print e
        print "发送失败"
        print e.message
        print str(e).decode('UTF-8')
        print (unicode(str(e),encoding="utf-8"))

#from杨秩  11:02:53
def send_sms(mobile_num,content):
    url= "http://51.123.123.15:234/s/sendSMSNew.m"
    data = urllib.urlencode({"content":'stp_code|'+content,
                             "typeNo":"00000","groupId":"000000","msgId":"0000000"})
    req = urllib2.Request(url,data)
    res = urllib2.urlopen(req)
    return res.read()

if __name__ == "__main__":

    send_sms("1300194200","你好呀！")
    
    # email(
    #      server="email.creditease.cn",
    #      port=25,
    #      msg_from="didiliu23@creditease.cn",
    #      passwd="DSFe123@sdfe",
    #      msg_to="123123@qq.com,didiliu23@creditease.cn",
    #      subject="邮件主题",
    #      content="邮件内容",
    #      attatch_file="out.jpg"
    #     )
