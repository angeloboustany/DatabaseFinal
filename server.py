import socket
import boto3
from tableCRUD import SynapseAI
import tableQuery
dynamodb = boto3.client('dynamodb', region_name='us-east-1')


def server_program():
    # get the hostname
    host = socket.gethostname()
    port = 5000  # initiate port no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(2)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        data = conn.recv(1024).decode()
        if not data:
            # if data is not received break
            break
        print("from connected user: " + str(data))
        # if data is equal to help show command list
        if data == 'help':
            msg = 'list of commands: 1 - exit - close connection / 2 - getitem - get item by a key / 3 - addPatient - add a new patient / 4 - delete - delete a table / 5 - create - create a table'
            conn.send(msg.encode())
        # if data is equal to exit close connection
        elif data == 'exit':
            conn.send('close connection'.encode())
            break
        # if data is equal to list getitem
        elif data == 'getitem':
            conn.send('Enter the following information: Table Name, KEY ex:"{\'X\':Y}"'.encode())
            data = conn.recv(1024).decode()
            #split only one the first comma
            data = data.split(',', 1)
            print(data)
            #convert string to dictionary
            data[1] = eval(data[1])
            try:
                res = sdb.getitem(data[0], data[1])
                conn.send(str(res).encode())
            except Exception as e:
                conn.send(f"Patient not found, something went wrong with the demo! Here's what: {e}".encode())
        # id data is equal to add: 'self,rid,scid,fn,ln,email,phone,availability,expertise,created_at' add a new patient
        elif data == 'addPatient':
            conn.send('Enter the following information: rid, scid, fn, ln, email, phone, availability, expertise, created_at'.encode())
            data = conn.recv(1024).decode()
            data = data.split(',')
            try:
                sdb.additem_Radiologist(int(data[0]), int(data[1]), data[2], data[3], data[4], int(data[5]), data[6], data[7], data[8])
                conn.send('Patient added'.encode())
            except Exception as e:
                conn.send("Patient not added, something went wrong with the demo! Here's what: {e}".encode())
        #if data is equal to delete: 'table' delete a table
        elif data == 'delete':
            conn.send('Enter the table name'.encode())
            data = conn.recv(1024).decode()
            try:
                sdb.delete_table(data)
                conn.send('Table deleted'.encode())
            except Exception as e:
                conn.send("Table not found, something went wrong with the demo! Here's what: {e}".encode())
        #if data is equal to create: 'table' create a table
        elif data == 'create':
            conn.send(
                'Enter the table: table_name, partitiom_key, att_type1, sort_key=Optional, att_type2=Optional'.encode())
            data = conn.recv(1024).decode()
            try:
                data = data.split(',')
                sdb.create_table(str(data[0]), str(data[1]), str(data[2]), str(data[3]), str(data[4]))
                conn.send('Table created'.encode())
            except Exception as e:
                conn.send("Table not created, something went wrong with the demo! Here's what: {e}".encode())
        else:
            conn.send('Invalid command'.encode())
    conn.close()  # close the connection

if __name__ == '__main__':
    try:
        dyn_res = boto3.resource('dynamodb')
        sdb = SynapseAI(dyn_res)
    except Exception as e:
        print(f"Something went wrong with the demo! Here's what: {e}")
    server_program()
