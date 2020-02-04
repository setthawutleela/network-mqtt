from socket import *
import sys, os

def addNewSock(list_socket_topic, topic, conn_sock, cli_sock_addr):
    if topic in list_socket_topic:
        list_socket_topic[topic].append({'socket': conn_sock, 'address': cli_sock_addr})
    else:
        list_socket_topic[topic] = [{'socket' : conn_sock, 'address': cli_sock_addr}]

def isSubscriber(message):
    return 'SUB' in message

def isPublisher(message):
    return 'PUB' in message

def getTopic(message):
    return message.split('\t')[1]

def getData(message):
    return message.split('\t')[2]

def getSocket(list_socket_topic, topic):
    return [item['socket'] for item in list_socket_topic[topic]]

def getAddr(list_socket_topic, topic):
    return [item['address'] for item in list_socket_topic[topic]]

def pairTopic(list_socket_topic, topic):
    return topic in list_socket_topic.keys()

def main():
    MAX_BUF = 2048
    SERV_PORT = 8000

    ip = str(sys.argv[1])

    addr = (ip, SERV_PORT)
    welcome_sock = socket(AF_INET, SOCK_STREAM)
    welcome_sock.bind(addr)
    welcome_sock.listen(100)
    print('TCP server started...')

    list_socket_topic = dict()

    while True:
        conn_sock, cli_sock_addr = welcome_sock.accept()
        print('New client connected from '+ str(cli_sock_addr[0]) + ':' + str(cli_sock_addr[1]))

        while True:
            msg = conn_sock.recv(MAX_BUF)
            msg_decoded = msg.decode('utf-8')
            print('MESSAGE: {}'.format(msg_decoded))
            
            if(isSubscriber(msg_decoded)):
                addNewSock(list_socket_topic, getTopic(msg_decoded), conn_sock, cli_sock_addr)
                print(list_socket_topic)
                print('=' * 60)
                break
            
            elif(isPublisher(msg_decoded)):
                if(pairTopic(list_socket_topic, getTopic(msg_decoded))):
                    sockets = getSocket(list_socket_topic, getTopic(msg_decoded))
                    addrs = getAddr(list_socket_topic, getTopic(msg_decoded))
                    print(sockets)
                    print(addrs)
                    for s in sockets:
                        s.send(getData(msg_decoded).encode('utf-8'))
                print('=' * 60)
                break

            else:
                print('WHAT THE ERROR!')
                sys.exit(0)
    welcome_sock.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print ('Interrupted ..')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)