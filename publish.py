from socket import *
import sys, os

def checkArgv(argv):
    """Check that arguments are correct in publisher form or not
        Step to check the argument for subscriber
        1. The amount of arguments
        2. The topic that is covered with single quote or is not
        3. The data that is covered with single quote or is not"""
    return len(argv) == 4  and (((len(argv[2]) > 2 and argv[2][0] == "'" and argv[2][len(argv[2])-1] == "'") or (len(argv[2]) > 0 and argv[2][0] != "'" and argv[2][len(argv[2])-1] != "'"))) and (((len(argv[3]) > 2 and argv[3][0] == "'" and argv[3][len(argv[3])-1] == "'") or (len(argv[3]) > 0 and argv[3][0] != "'" and argv[3][len(argv[3])-1] != "'")))

def getTopic(topic):
    """Change topic into the correct form"""
    if (topic[0] == "'" and topic[len(topic)-1] == "'"):
        return topic[1:len(topic) - 1]
    else:
        return topic

def getData(data):
    """Change data into the correct form"""
    if (data[0] == "'" and data[len(data)-1] == "'"):
        return data[1:len(data) - 1]
    else:
        return data

def main():
    # Check the argument form
    if (checkArgv(sys.argv) == False):
        print('\nPlease enter correct usage:')
        print('\n\tpublish [IP address] [Topic] [Data]')
        print('\n\t\tor')
        print("\n\tpublish [IP address] '[Topic]' '[Data]'")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    ip = str(sys.argv[1])
    topic = getTopic(str(sys.argv[2]))
    data = getData(str(sys.argv[3]))

    print('')
    print('IP address: ', ip)
    print('Topic: ', str(topic))
    print('Data: ', str(data))

    addr = (ip, SERV_PORT)
    
    # Connect the broker
    try:
        s.connect(addr)
    except ConnectionRefusedError:
        print('Error! connecting to {} failed'.format(addr))
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

    # Send the topic and data to the connected broker
    s.send(('PUB\t'+topic+'\t'+data).encode('utf-8'))

    #s.setblocking(0)
    
    s.close()
    print('Shutdown publisher..')

if __name__ == '__main__':
    try:
        MAX_BUF = 2048
        SERV_PORT = 8000
        s = socket(AF_INET, SOCK_STREAM)
        main()
    except KeyboardInterrupt:
        print ('Program interrupted..')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)