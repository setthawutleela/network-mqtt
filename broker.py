from socket import *
from threading import Thread
import sys, os

def addConSock(list_socket_topic, topic, conn_sock, cli_sock_addr):
    """Store the information about connecting socket from subscriber"""
    if topic in list_socket_topic:
        list_socket_topic[topic].append({'conn_sock': conn_sock, 'address': cli_sock_addr})
    else:
        list_socket_topic[topic] = [{'conn_sock' : conn_sock, 'address': cli_sock_addr}]

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
    return [item['conn_sock'] for item in list_socket_topic[topic]]

def getAddr(list_socket_topic, topic):
    """Retrieve the address that match with the topic"""
    return [item['address'] for item in list_socket_topic[topic]]

def pairTopic(list_socket_topic, topic):
    """Match topic with the stored topics"""
    return topic in list_socket_topic.keys()

def shutdown_connection():
    """Shutdown all connected sockets"""
    for list_conn_sock in list_socket_topic.values():
        for conn_sock in list_conn_sock:
            conn_sock['conn_sock'].send(('SHUTDOWN_CONN').encode('utf-8'))
            # print IP address and port of connected socket
            print('CLOSED '+str(conn_sock['address'][0])+':'+str(conn_sock['address'][1])) 

def delSocket(conn_sock):
    """Delete connected sockets"""
    for list_conn_sock in list_socket_topic.values():
        for i in range(len(list_conn_sock)):
            if (list_conn_sock[i]['conn_sock'] == conn_sock):
                print('CLOSED '+str(list_conn_sock[i]['address'][0])+':'+str(list_conn_sock[i]['address'][1]))
                del list_conn_sock[i]
                return

def handle_client(conn_sock, cli_sock_addr, welcome_sock, ):
    while True:
        try:
            msg = conn_sock.recv(MAX_BUF)

            # Get some message from connected socket
            if(len(msg) > 0):    
                msg_decoded = msg.decode('utf-8')

                # If that message comes from subscriber
                if(isSubscriber(msg_decoded)):
                    # If subscriber is closed
                    if('DEL_SOCKET' == msg_decoded.split('\t')[1]):
                        delSocket(conn_sock)
                    
                    # If new subscriber subscribes the broker
                    else:
                        print(msg_decoded.split('\t')[0]+' '+getTopic(msg_decoded)+'\t'+str(cli_sock_addr[0])+':'+str(cli_sock_addr[1]))
                        addConSock(list_socket_topic, getTopic(msg_decoded), conn_sock, cli_sock_addr)
                
                
                # If that message comes from publisher
                elif(isPublisher(msg_decoded)):
                    print(msg_decoded.split('\t')[0]+' '+getTopic(msg_decoded)+' '+getData(msg_decoded)+'\t'+str(cli_sock_addr[0])+':'+str(cli_sock_addr[1]))
                    
                    # Publisher's topic matches with Subscriber's topic
                    if(pairTopic(list_socket_topic, getTopic(msg_decoded))):

                        #If publisher sends the message for subscriber to disconnect broker
                        if('QUIT' == getData(msg_decoded).upper()):
                            sockets = getSocket(list_socket_topic, getTopic(msg_decoded))
                            addresses = getAddr(list_socket_topic, getTopic(msg_decoded))
                            for s, a in zip(sockets, addresses):
                                s.send(('SHUTDOWN_CONN').encode('utf-8'))
                                print('CLOSED '+str(a[0])+':'+str(a[1]))
                            del list_socket_topic[getTopic(msg_decoded)]
                    
                        #If publisher wants to send the message to matched topics
                        else:
                            sockets = getSocket(list_socket_topic, getTopic(msg_decoded))
                            addresses = getAddr(list_socket_topic, getTopic(msg_decoded))
                            for s, a in zip(sockets, addresses):
                                s.send((getData(msg_decoded)).encode('utf-8'))
                        
                        conn_sock.close()
                    break

        except BlockingIOError:
            pass

def main():
    # Check the argument form
    if (len(sys.argv) != 2):
        print('\nPlease enter correct usage:')
        print('\n\t broker [IP address]')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    ip = str(sys.argv[1])

    addr = (ip, SERV_PORT)
    
    try:
        welcome_sock.bind(addr)
    except OSError:
        print('\nError! {} was already used'.format(addr))
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
        
    welcome_sock.listen(5)

    print('\nTCP server is running on '+ str(ip) + ':' + str(SERV_PORT))

    welcome_sock.setblocking(0)

    while True:
        try:
            conn_sock, cli_sock_addr = welcome_sock.accept()

            # Create thread to handle the new client
            try:
                Thread(target=handle_client, args=(conn_sock, cli_sock_addr, welcome_sock, )).start()
            
            # Threading failed
            except:
                print("Cannot start thread..")
                import traceback
                # Give the error
                traceback.print_exc() 
        except BlockingIOError:
            pass


if __name__ == '__main__':
    try:
        SERV_PORT = 8000
        MAX_BUF = 2048
        list_socket_topic = dict()
        welcome_sock = socket(AF_INET, SOCK_STREAM)
        main()
    except KeyboardInterrupt:
        print ('Program interrupted..')
        shutdown_connection()
        welcome_sock.close()
        print('Shutdown broker..')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)