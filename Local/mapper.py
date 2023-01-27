import config
import socket
import threading
from collections import defaultdict


def mapper_main():

    def word_count(data):
        output=[]
        for datum in data.values():
            for word in datum.split():
                output.append((word,'1'))
        return output

    def inverted_index(data):
        output=[]
        for k,v in data.items():
            for word in v.split():
                output.append((word,k))
        return output

    def connect_reducer(server,client,id):

        mr_dict={"id":str(id)}

        while True:
            command = client.recv(1024)
            if command:
                command = command.decode("utf-8").split()
                if command[0] == 'quit':
                    client.send(bytes('transmission done',"utf-8"))
                    break
                if command[0]=='set':
                    try:
                        mr_dict[command[1]]=command[-1]
                        client.send(bytes('STORED\r\n',"utf-8"))
                    except:
                        client.send(bytes('NOT-STORED\r\n',"utf-8"))
                elif command[0]=='get':
                    client.send(bytes("VALUE "+command[1]+" "+mr_dict.get(command[1],'not_set')+" END","utf-8"))
                elif command[0]=='get_all':
                    for w,v in output:
                        if w and ((n_reducers+1)*ord(w[0]))%n_reducers == int(mr_dict['id_r']):
                            client.send(bytes(str(w+' '+str(v)),"utf-8"))
                            command=client.recv(1024).decode("utf-8")
                    client.send(bytes(str('/0'),"utf-8"))
                else:
                    break

        reducers_exec_check.append(mr_dict['id_r'])


    #start main program
    ip = config.main["ip"]
    port = config.main["port"]
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((ip, port))

    #get mapper id
    server.send(bytes('get id','utf-8'))
    id=int(server.recv(1024).decode("utf-8").split()[2])

    #get mapper functions
    server.send(bytes('get app','utf-8'))
    func=server.recv(1024).decode("utf-8").split()[2]

    #get n_reducers functions
    server.send(bytes('get n_reducers','utf-8'))
    n_reducers=int(server.recv(1024).decode("utf-8").split()[2])

    
    # get data
    data=defaultdict(str)
    server.send(bytes('get_all','utf-8'))
    while True:
        doc_id=server.recv(4096).decode("utf-8")
        if doc_id != '/0':
            server.send(bytes(str('done'),'utf-8'))
            temp=''
            while True:
                buff=server.recv(4096).decode("utf-8")
                if buff == '/0':
                    server.send(bytes(str('done'),'utf-8'))
                    break
                temp+=buff
                server.send(bytes(str('more'),'utf-8'))
            data[doc_id]=temp
        else:
            server.send(bytes(str('trasmission done'),'utf-8'))
            break
    
    #set mapper id
    server.send(bytes('set id_m '+str(id),'utf-8'))
    response = server.recv(1024).decode("utf-8")

    #quit and get confirmation
    server.send(bytes('quit','utf-8'))
    response = int(server.recv(1024).decode("utf-8"))

    #below 3 lines of code is used to check simulated fault tolerance of mappers
    if not response:
        server.close()
        return
    
    if func=="word_count":
        output=word_count(data)
    elif func=="inverted_index":
        output=inverted_index(data)

    # conect with reducers
    ip_m = ip
    port_m = port+id+1
    server_m = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_m.bind((ip_m, port_m))
    server_m.listen(5)

    reducers_threads = [None] * n_reducers
    reducers_exec_check=[]
    for r in range(n_reducers):
        #connect to reducer
        client, address = server_m.accept()
        reducers_threads[r] = threading.Thread(target=connect_reducer, \
            args=(server_m,client,id))
        # reducers_threads[r].setDaemon(True)
        reducers_threads[r].start()
    for i in range(n_reducers):
        reducers_threads[i].join()

    server.close()
    server_m.close()