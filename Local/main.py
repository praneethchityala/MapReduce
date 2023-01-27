import socket
import signal
import threading
import config
import mapper
import reducer
import random

def sig_handler(signum, frame):
    # exit(1)
    pass


def fire_mapper(server,id,data_m):
    try:
        m_dict={"id":str(id),"app":app,"n_reducers":str(n_reducers),'data':data_m}

        #fire mapper
        thread_c = threading.Thread(target=mapper.mapper_main, args=())
        # thread_c.setDaemon(True)
        thread_c.start()

        #connect to mapper
        client, address = server.accept()
        while True:
            command = client.recv(1024)
            if command:
                command = command.decode("utf-8").split()
                if command[0] == 'quit':
                    break
                if command[0]=='set':
                    try:
                        m_dict[command[1]]=command[-1]
                        client.send(bytes('STORED\r\n',"utf-8"))
                    except:
                        client.send(bytes('NOT-STORED\r\n',"utf-8"))
                elif command[0]=='get':
                    client.send(bytes("VALUE "+command[1]+" "+m_dict.get(command[1],'not_set')+" END","utf-8"))
                elif command[0]=='get_all':
                    for k,v in m_dict['data'].items():
                        N=v[0]
                        q=N//n_mappers
                        data=' '.join(v[1][id*q:min((id+1)*q,N+1)])
                        n=len(data)
                        client.send(bytes(str(k),"utf-8"))
                        command = client.recv(1024)
                        for i in range(1+n//4096):
                            client.send(bytes(data[i*4096:min((i+1)*4096,n)],"utf-8"))
                            command = client.recv(1024)
                        client.send(bytes('/0',"utf-8"))
                        command = client.recv(1024)
                    client.send(bytes('/0',"utf-8"))
                    command = client.recv(1024)
                else:
                    break

        #below 3 lines of code is used to check simulated fault tolerance of mappers
        if fail_mappers:
            if random.randrange(0, 10)%2==0:
                m_dict["id_m"]=None
        if m_dict["id_m"] != str(id):
            client.send(bytes(str(0),"utf-8"))
            print('mapper ',id,' failed, restarting mapper ',id)
            fire_mapper(server,id,data_m)
        else:
            client.send(bytes(str(1),"utf-8"))
            mapper_data_check.append(m_dict["id_m"])
    except:
        print('mapper ',id,' failed, restarting mapper ',id)
        fire_mapper(server,id,data_m)

def fire_reducer(server,id,func):

    try:
        r_dict={"id":str(id),"app":app,"n_mappers":str(n_mappers)}

        #fire mapper
        thread_r = threading.Thread(target=reducer.reducer_main, args=())
        # thread_r.setDaemon(True)
        thread_r.start()

        #connect to reducer
        client, address = server.accept()

        #below 7 lines of commented code is used to check simulated fault tolerance of reducers
        if fail_reducers:
            if random.randrange(0, 10)%2==0:
                client.send(bytes('close',"utf-8"))
                command = client.recv(1024)
                print('reducer ',id,' failed, restarting reducer ',id)
                client.close()
                fire_reducer(server,id,func)
                return
        else:
            client.send(bytes('continue',"utf-8"))
            

        while True:
            command = client.recv(1024)
            if command:
                command = command.decode("utf-8").split()
                if command[0] == 'quit':
                    break
                if command[0]=='set':
                    try:
                        r_dict[command[1]]=command[-1]
                        client.send(bytes('STORED\r\n',"utf-8"))
                    except:
                        client.send(bytes('NOT-STORED\r\n',"utf-8"))
                elif command[0]=='get':
                    client.send(bytes("VALUE "+command[1]+" "+r_dict.get(command[1],'not_set')+" END","utf-8"))
                elif command[0]=='set_all':
                    client.send(bytes('setting data','utf-8'))
                    while True:
                        datum=client.recv(4096).decode("utf-8")
                        if datum=='/0':
                            client.send(bytes('done','utf-8'))
                            break
                        elif len(datum.split())==2:
                            w,v=datum.split()
                            output[w]=v
                            client.send(bytes('more','utf-8'))
                        else:
                            client.send(bytes('more','utf-8'))
                else:
                    break

        client.send(bytes(str(1),"utf-8"))
        reducers_exec_check.append(r_dict["id_r"])
            
    except:
        print('reducer ',id,' failed, restarting reducer ',id)
        fire_reducer(server,id,func)


if __name__ == '__main__':

    signal.signal(signal.SIGINT, sig_handler)
    
    data={}
    n_mappers=config.main["mappers"]
    n_reducers=config.main["reducers"]
    app=config.main["application"]
    input_=config.main["input"]
    output_=config.main["output"]
    ip = config.main["ip"]
    port = config.main["port"]
    fail_mappers=config.main["fail_random_mappers"]
    fail_reducers=False

    if n_mappers < 1 or type(n_mappers) is not int:
        print("mappers number is either 0 or not int. Please provde int > 0 in config.py")
        exit(1)
    
    if n_reducers < 1 or type(n_reducers) is not int:
        print("reducers number is either 0 or not int. Please provde int > 0 in config.py")
        exit(1)

    if app != 'word_count' and app != 'inverted_index':
        print("\nApplication must be either word_count or inverted_index.\n Please check config.py.\n")
        exit(1)

    for i in range(len(input_)):
        f = open(input_[i],'r')
        d=f.read()
        f.close()
        temp=d.split('\n')
        data[i]=(len(temp),temp)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.bind((ip, port))
    except:
        print('please choose another port, choosen port is unavailable')
        exit(1)
    server.listen(5)

    mappers_threads = [None] * n_mappers
    mapper_data_check=[]

    print('\nmapper workers on work....\n')

    for m in range(n_mappers):
        mappers_threads[m] = threading.Thread(target=fire_mapper, \
            args=(server,m,data))
        # mappers_threads[m].setDaemon(True)
        mappers_threads[m].start()
    
    

    for i in range(n_mappers):
        mappers_threads[i].join()
    
    output={}

    if len(mapper_data_check) == n_mappers:
        print('mappers completion order : ',mapper_data_check,'\n')
        print('reducer workers on work....\n')
        reducers_threads = [None] * n_reducers
        reducers_exec_check=[]
        for r in range(n_reducers):
            reducers_threads[r] = threading.Thread(target=fire_reducer, \
                args=(server,r,app))
            # reducers_threads[r].setDaemon(True)
            reducers_threads[r].start()
        for i in range(n_reducers):
            reducers_threads[i].join()
        print('reducers completion order : ',reducers_exec_check,'\n')
        f=open(output_,'w')
        f.write(str(output))
        f.close()
        print(app,' k-v pairs stored!\n')
        server.close()

