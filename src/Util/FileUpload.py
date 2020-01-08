import paramiko

hostname = '47.56.134.201'            #此处是linux的ip

port = 22

username = 'root'

password = 'liumengli129!'

t = paramiko.Transport((hostname, port))    #实现ftp功能，声明ftp实例

t.connect(username=username, password=password)

sftp = paramiko.SFTPClient.from_transport(t)

sftp.put(r'D:\python\PyCharm 2019.1.2\workspace\DigitalCurrency.tar.gz', '/usr/local/program/DigitalCurrency.tar.gz')  #上传和下载只需要更改前后顺序

sftp.close()