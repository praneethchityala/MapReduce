import config
import socket
import threading
from collections import defaultdict


def reducer_main():

    def mapper_connect(ip,m,id):
        ip_m = ip
        port_m = port+m+1
        server_m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_m.connect((ip_m, port_m))

        #set and confirm reducer id
        server_m.send(bytes('set id_r '+str(id),'utf-8'))
        response=server_m.recv(1024).decode("utf-8")

        #get mapper id
        server_m.send(bytes('get id','utf-8'))
        id_m=int(server_m.recv(1024).decode("utf-8").split()[2])

        #fault tolerance: incase reducer is not connected to correct mapper
        if id_m!=m:
            server_m.close()
            return mapper_connect(ip,m,id)
        
        # recieve data from mapper
        server_m.send(bytes('get_all','utf-8'))
        if func=='word_count':
            while True:
                datum=server_m.recv(4096).decode("utf-8")
                if datum=='/0':
                    break
                elif datum:
                    w,v=datum.split()
                    output[w]+=1
                    server_m.send(bytes('more','utf-8'))
        elif func=='inverted_index':
            while True:
                datum=server_m.recv(4096).decode("utf-8")
                if datum=='/0':
                    break
                elif datum:
                    w,v=datum.split()
                    output[w].add(v)
                    server_m.send(bytes('more','utf-8'))
        
        #end of trasmission
        server_m.send(bytes('quit','utf-8'))
        response=server_m.recv(1024).decode("utf-8")

        mapper_data_check.append(m)



    #start main program
    ip = config.main["ip"]
    port = config.main["port"]
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((ip, port))

    #simulated fault tolerance check
    check=server.recv(1024).decode("utf-8")
    if check=='close':
        server.send(bytes('ok','utf-8'))
        server.close()
        return
    
    #get and confirm reducer id
    server.send(bytes('get id','utf-8'))
    id=int(server.recv(1024).decode("utf-8").split()[2])

    #get and confirm reducer functions
    server.send(bytes('get app','utf-8'))
    func=server.recv(1024).decode("utf-8").split()[2]

    #get mappers information
    server.send(bytes('get n_mappers','utf-8'))
    n_mappers=int(server.recv(1024).decode("utf-8").split()[2])

    if func=='word_count':
        output=defaultdict(int)
    elif func=='inverted_index':
        output=defaultdict(set)


    # connections with mappers
    mappers_threads = [None] * n_mappers
    mapper_data_check=[]

    for m in range(n_mappers):
        mappers_threads[m] = threading.Thread(target=mapper_connect, \
            args=(ip,m,id))
        # mappers_threads[m].setDaemon(True)
        mappers_threads[m].start()
    
    for i in range(n_mappers):
        mappers_threads[i].join()

    if len(mapper_data_check)==n_mappers:
        # send data to main for consolidation
        server.send(bytes('set_all','utf-8'))
        command=server.recv(1024).decode("utf-8")
        if func=='word_count':
            for k,v in output.items():
                server.send(bytes(k+' '+str(v),'utf-8'))
                command=server.recv(1024).decode("utf-8")
        elif func=='inverted_index':
            for k,v in output.items():
                server.send(bytes(k+' '+','.join(v),'utf-8'))
                command=server.recv(1024).decode("utf-8")
        
        server.send(bytes(str('/0'),'utf-8'))
        command=server.recv(1024).decode("utf-8")
    
    #set reducer id
    server.send(bytes('set id_r '+str(id),'utf-8'))
    response = server.recv(1024).decode("utf-8")

    #quit and get confirmation
    server.send(bytes('quit','utf-8'))
    response = int(server.recv(1024).decode("utf-8"))

    server.close()

