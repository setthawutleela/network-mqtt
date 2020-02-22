from socket import *
import sys, os    

def checkArgv(argv):
    """Check that arguments are correct in subscriber form or not
        Step to check the argument for subscriber
        1. The amount of arguments
        2. The topic that is covered with single quote or is not"""
    return len(argv) == 3  and (((len(argv[2]) > 2 and argv[2][0] == "'" and argv[2][len(argv[2])-1] == "'") or (len(argv[2]) > 0 and argv[2][0] != "'" and argv[2][len(argv[2])-1] != "'")))

def getTopic(topic):
    """Change topic into the correct form"""
    if (topic[0] == "'" and topic[len(topic)-1] == "'"):
        return topic[1:len(topic) - 1]
    else:
        return topic

def delSocket():
    """Delete this socket that is kept in broker"""
    s.send(('SUB\tDEL_SOCKET').encode('utf-8'))
    s.close()
    print('Shutdown socket..')

def main():
    # Check the argument form
    if (checkArgv(sys.argv) == False):
        print('\nPlease enter correct usage:')
        print('\n\t subscribe [IP address] [Topic]')
        print('\n\t\tor')
        print("\n\t subscribe '[IP address]' '[Topic]'")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    ip = str(sys.argv[1])
    topic = getTopic(str(sys.argv[2]))

    print()
    print('IP address: ' + ip)
    print('Topic: '+ topic)

    addr = (ip, SERV_PORT)
    
    try:
        s.connect(addr)
    except ConnectionRefusedError:
        print('Error! connecting to {} failed'.format(addr))
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    # Send the topic to the connected broker
    s.send(('SUB\t'+topic).encode('utf-8'))

    s.setblocking(0)

    while True:
        try:
            msg = s.recv(MAX_BUF)
            if(len(msg) > 0):
                msg_decoded = msg.decode('utf-8')
                
                # Broker is closed
                if('SHUTDOWN_CONN' == msg_decoded):
                    s.close()
                    print('Shutdown subscriber..')
                    break

                # Publisher published the message
                else:
                    print('PUB '+msg_decoded)
                    
        except BlockingIOError:
            pass
        except:
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    try:
        MAX_BUF = 2048
        SERV_PORT = 8000
        s = socket(AF_INET, SOCK_STREAM)
        main()
    except KeyboardInterrupt:
        print ('Program interrupted..')
        delSocket()
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)