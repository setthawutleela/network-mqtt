#test git by wannpt
from socket import *
import sys

def isSubscriber(message):
    return 'SUB' in message

def isPublisher(message):
    return 'PUB' in message

def getTopic(message):
    return message.split('\t')[1]

def getData(message):
    return message.split('\t')[2]

def pairTopic(list_socket_topic, topic):
    return topic in list_socket_topic.values()

def getSocket(list_socket_topic, topic):
    return [s for s, t in list_socket_topic.items() if t == topic]

def main():
    MAX_BUF = 2048
    SERV_PORT = 8000

    ip = str(sys.argv[1])

    addr = (ip, SERV_PORT)
    welcome_sock = socket(AF_INET, SOCK_STREAM)
    welcome_sock.bind(addr)
    welcome_sock.listen(100)
    print('TCP server started...')

    list_socket_topic = {}

    while True:
        conn_sock, cli_sock_addr = welcome_sock.accept()
        print('New client connected...')

        while True:
            msg = conn_sock.recv(MAX_BUF)
            msg_decoded = msg.decode('utf-8')
            print('MESSAGE: {}'.format(msg_decoded))
            print('CONNECTED SOCKET: {}'.format(cli_sock_addr))
            
            if(isSubscriber(msg_decoded)):
                list_socket_topic.update({cli_sock_addr: getTopic(msg_decoded)})
                print(list_socket_topic)
                print('=' * 60)
                conn_sock.send('')
                conn_sock.close()
                break
            
            elif(isPublisher(msg_decoded)):
                if(pairTopic(list_socket_topic, getTopic(msg_decoded))):
                    sockets = getSocket(list_socket_topic, getTopic(msg_decoded))
                    for s in sockets:
                        #
                        # Cannot connect the socket that its topic match the publisher's topic!!!!
                        #
                print('=' * 60)
                break
            
            else:
                print('WHAT THE ERROR!')
        conn_sock.close()
    welcome_sock.close()

if __name__ == '__main__':
    main()