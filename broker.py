from socket import *
import sys, os

def addConSock(list_socket_topic, topic, conn_sock, cli_sock_addr):
    """Store the information about connecting socket from subscriber"""
    if topic in list_socket_topic:
        list_socket_topic[topic].append({'socket': conn_sock, 'address': cli_sock_addr})
    else:
        list_socket_topic[topic] = [{'socket' : conn_sock, 'address': cli_sock_addr}]

def isSubscriber(message):
    """Check that the connecting socket is subscriber"""
    return 'SUB' in message

def isPublisher(message):
    """Check that the connecting socket is publisher"""
    return 'PUB' in message

def getTopic(message):
    """Retrieve the topic from the message"""
    return message.split('\t')[1]

def getData(message):
    """Retrieve the data from the message"""
    return message.split('\t')[2]

def getSocket(list_socket_topic, topic):
    """Retrieve the sockets that match with the topic"""
    return [item['socket'] for item in list_socket_topic[topic]]

def getAddr(list_socket_topic, topic):
    """Retrieve the address that match with the topic"""
    return [item['address'] for item in list_socket_topic[topic]]

def pairTopic(list_socket_topic, topic):
    """Match topic with the stored topics"""
    return topic in list_socket_topic.keys()

def main():
    MAX_BUF = 2048
    SERV_PORT = 8000

    ip = str(sys.argv[1])

    addr = (ip, SERV_PORT)
    welcome_sock = socket(AF_INET, SOCK_STREAM)
    welcome_sock.bind(addr)
    welcome_sock.listen(1)
    print('TCP server is running on {}'.format(addr))

    list_socket_topic = dict()

    while True:
        conn_sock, cli_sock_addr = welcome_sock.accept()
        print('CONNECTED '+ str(cli_sock_addr[0]) + ':' + str(cli_sock_addr[1]))

        while True:
            msg = conn_sock.recv(MAX_BUF)
            msg_decoded = msg.decode('utf-8')
            
            if(isSubscriber(msg_decoded)):
                print(msg_decoded.split('\t')[0]+' '+getTopic(msg_decoded))
                addConSock(list_socket_topic, getTopic(msg_decoded), conn_sock, cli_sock_addr)
                break
            
            elif(isPublisher(msg_decoded)):
                print(msg_decoded.split('\t')[0]+' '+getTopic(msg_decoded)+' '+getData(msg_decoded))
                if(pairTopic(list_socket_topic, getTopic(msg_decoded))):
                    sockets = getSocket(list_socket_topic, getTopic(msg_decoded))
                    addresses = getAddr(list_socket_topic, getTopic(msg_decoded))
                    for s, a in zip(sockets, addresses):
                        s.send(getData(msg_decoded).encode('utf-8'))
                        print('REL {}: {}\t{}'.format(a, getTopic(msg_decoded), getData(msg_decoded)))
                break

            else:
                print('ERROR! EXIT THE PROGRAM')
                sys.exit(0)
                
    welcome_sock.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print ('Program interrupted..')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)