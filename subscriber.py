from socket import *
import sys, os

def main():
    MAX_BUF = 2048
    SERV_PORT = 8000

    ip = str(sys.argv[1])
    topic = str(sys.argv[2])

    print('IP address: ' + ip)
    print('Topic: '+ topic)

    addr = (ip, SERV_PORT)
    s = socket(AF_INET, SOCK_STREAM)
    s.connect(addr)
    s.send(('SUB\t'+topic).encode('utf-8'))

    while True:
        msg = s.recv(MAX_BUF)
        if(len(msg) > 0):
            print('Publisher> %s'%(msg.decode('utf-8')))

    s.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print ('Interrupted ..')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)