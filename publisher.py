from socket import *
import sys

SERV_PORT = 8000

ip = str(sys.argv[1])
topic = str(sys.argv[2])
data = str(sys.argv[3])

print('IP address: ', ip)
print('Topic: ', str(topic))
print('Data: ', str(data))

addr = (ip, SERV_PORT)
s = socket(AF_INET, SOCK_STREAM)
s.connect(addr)
s.send(('PUB\t'+topic+'\t'+data).encode('utf-8'))

s.close()