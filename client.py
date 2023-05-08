import socket
import time


def client_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number

    client_socket = socket.socket()  # instantiate
    # mesure the time that took to connect to the server
    start = time.time()
    client_socket.connect((host, port))  # connect to the server
    end = time.time()

    print('Connection established with host: ' + host + ' and port: ' + str(port) + 'in ' + str(end - start) + 's')
    print('Type help to show list of commands' + '\n' + 'Type exit to close connection')

    message = input(" -> ")  # take input

    while message.lower().strip() != 'quit':
        client_socket.send(message.encode())  # send message
        data = client_socket.recv(1024).decode()  # receive response

        print('Received from server: ' + data)  # show in terminal

        message = input(" -> ")  # again take input

    client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()
