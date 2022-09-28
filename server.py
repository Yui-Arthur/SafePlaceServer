from ast import Bytes
from datetime import datetime,timezone,timedelta
import socket
import threading
import socketserver
import time 
import os
from urllib import response
import uuid
from typing import Generator
import csv
import pg8000
from rsa import PublicKey, verify
import sqlalchemy
from google.cloud.sql.connector import Connector
import time
from Crypto.PublicKey import RSA
import pyotp
import psycopg2



# SQL table                      
                       
# Store     (Save all Store Info)
# uid | name | type | information | lat | lng | ts | te
# CREATE TABLE store_data( 
# ID  INT PRIMARY KEY NOT NULL,
# NAME VARCHAR(20) NOT NULL,
# TYPE VARCHAR(10) NOT NULL,
# INFO VARCHAR(50) NOT NULL,
# LNG DOUBLE PRECISION NOT NULL,
# LAT DOUBLE PRECISION NOT NULL,
# TS INT NOT NULL,
# TE INT NOT NULL
# );

# user_position  (Save user's location)
# id | position | time
# CREATE TABLE user_position(
# id INT NOT NULL,
# position VARCHAR(100) NOT NULL,
# time TIME WITH TIME ZONE NOT NULL
# );

# otp_position (Save otp)
# CREATE TABLE otp_position(
# otp VARCHAR(10) PRIMARY KEY NOT NULL,
# id INT NOT NULL
# );

# Report    (Save user's report and newst)
# uid | Reason
# CREATE TABLE report_store (
# ID INT REFERENCES store_data(id) NOT NULL,
# REASON VARCHAR(20) NOT NULL
# );

# NEWST     (Save store user want add)
# name | type | information | location | time
# CREATE TABLE new_store(
# NAME VARCHAR(20) NOT NULL,
# TYPE VARCHAR(10) NOT NULL,
# INFO VARCHAR(50) NOT NULL,
# LOCATION VARCHAR(200) NOT NULL,
# TS INT NOT NULL,
# TE INT NOT NULL
# );


# UPDATE
# Check or Updata store data 
# Format 
# Client UPDATE CurrentVersion
# Server DELETE uid 


# REPORT
# Report the Wrong Store info(Time or Not Exist ?)
# Format
# Client REPORT uid Reason
# Server X

# SHARE
# Upload user's location(encryption RSA ?) to SQL and give a uniqe uid 
# And Give the uid to Emergenct Contact to Get the location
# Server will store the uid about 10~30 min
# User will Uplad location about 30~60 second
# After that SQL will Clean the data
# Format
# Client SHARE uid(First 0) location
# Server X(First Uid)


# GETLOC
# Get a user's location 
# Format 
# Client GETLOC uid
# Server location(encryption?) or Error


# NEWST
# Add a new store 
# Format 
# Client NEWST StoreInfo
# Server X


class SafePlaceThreadedTCPServerHandler(socketserver.BaseRequestHandler):
    def  handle(self):
        
        s=str(self.request.recv(1024), 'utf8')
        print(s)
        operation=s.split('|')[0]
        data = s.split('|')
        #cur_thread = threading.current_thread()
        #response = bytes("{}: {}".format(cur_thread.name, data), 'ascii')
        print("Thread count "+str(threading.active_count()))
        


        if operation=="UPDATE":
            response = update_version(data)
        elif operation=="DOWNLOAD":
            response = download_store_data(data)
        elif operation=="REPORT":
            response = report_store(data)
        elif operation=="SHARE":
            response = share_location(data)
        elif operation=="GETLOC":
            response = get_location(data)
        elif operation=="NEWST":
            response = new_store(data)
        elif operation=="VYOTP":
            response = verify_otp(data)
        else:
            s=bytes("Error", 'utf8')
            response = [len(s).to_bytes(4, byteorder='big'),s]

        # print(response[1])
        self.request.sendall(response[0])
        self.request.sendall(response[1])
        

        

def update_version(data):

    user_version=data[1]

    with open('data_version','r') as version_list:

        version_change_li=[0,0,0]
        for version in version_list.readlines():
            latest_version=version
            version=version.strip('\n')
            if user_version < version:
                with open(version,'r') as version_file:
                    now=0
                    for i in version_file.readlines():
                        i=i.strip('\n')
                        if i == "ADD":
                            now=0
                        elif i == "DELETE":
                            now=1
                        elif i== "MODIFY":
                            now=2
                        else:
                            version_change_li[now]+=1

    return_data=latest_version+" "+"ADD "+str(version_change_li[0])+" DELETE "+str(version_change_li[1])+" MODIFY "+str(version_change_li[2])
    return_data=bytes(return_data,'utf8')
    size=len(return_data).to_bytes(4, byteorder='big')


    return [size,return_data]
                        
                

def download_store_data(data):
    pool = psycopg2.connect(database="SafePlace", user="postgres", password="1091102", host="127.0.0.1", port="5432")
    
    with pool.cursor() as db_conn:
        
        if data[1]=='v0':
                
            db_conn.execute("SELECT * from store_data ORDER BY id ASC")
            rel=db_conn.fetchall()
            ret="ADD "+str(len(rel))+"\n"


            for row in rel:
                for col in row:
                    
                    ret+=str(col)+"|"
                ret+="\n"
            
            ret+="DELETE 0\n"
            ret+="MODIFY 0\n"
        else:
            with open('data_version','r') as version_list:
                user_version=data[1]
                version_change_li=[0,0,0]
                change={}
                for version in version_list.readlines():
                    version=version.strip('\n')
                    if user_version < version:
                        with open(version,'r') as version_file:
                            now=0
                            for i in version_file.readlines():
                                i=i.strip('\n')
                                if i == "ADD":
                                    now=0
                                elif i == "DELETE":
                                    now=1
                                elif i== "MODIFY":
                                    now=2
                                else:
                                    if i in change and now==1:
                                        change.pop(i)
                                        
                                    else:
                                        change[i]=now
                                        version_change_li[now]+=1

           
            
            add_li=[]
            delete_li=[]
            modify_li=[]
            for id in change:
                if change[id]==0:
                    db_conn.execute(f"SELECT * from store_data WHERE id={id}")
                    rel=db_conn.fetchall()
                    add_li.append(rel)
                elif change[id]==1:
                    delete_li.append(id)
                elif change[id]==2:
                    rel=db_conn.execute(f"SELECT * from store_data WHERE id={id}")
                    rel=db_conn.fetchall()
                    modify_li.append(rel)

            ret="ADD "+str(len(add_li))+"\n"

            for data in add_li:
                # print(row)
                for row in data:
                    for col in row:
                        ret+=str(col)+"|"
                ret+="\n"

            ret+="DELETE "+str(len(delete_li))+"\n"

            for id in delete_li:
                ret+=id+"\n"

            ret+="MODIFY "+str(len(modify_li))+"\n"

            for row in modify_li:
                # print(row)
                for row in data:
                    for col in row:
                        ret+=str(col)+"|"
                ret+="\n"


                


       
        db_conn.close()

    

    #print(ret)
    return_data=bytes(ret,'utf8')
    size=len(return_data).to_bytes(4,byteorder='big')
    return [size,return_data]

def report_store(data):
    
    pool = psycopg2.connect(database="SafePlace", user="postgres", password="1091102", host="127.0.0.1", port="5432")
    with pool.cursor() as db_conn:
        
        try:
            db_conn.execute(f"INSERT INTO report_store (ID,REASON) VALUES ({data[1]},'{data[2]}')")
            pool.commit()
        except:
            db_conn.close()
            return_data=bytes("Error",'utf8')
            size=len(return_data).to_bytes(4,byteorder='big')
            return [size,return_data]

        db_conn.close()

    return_data=bytes("Success",'utf8')
    size=len(return_data).to_bytes(4,byteorder='big')
    return [size,return_data]

def new_store(data):

    pool = psycopg2.connect(database="SafePlace", user="postgres", password="1091102", host="127.0.0.1", port="5432")
    with pool.cursor() as db_conn:
        
        try:
            db_conn.execute(f"INSERT INTO new_store (NAME,TYPE,INFO,LOCATION,TS,TE) VALUES ('{data[1]}','{data[2]}','{data[3]}','{data[4]}',{data[5]},{data[6]})")
            pool.commit()
        except:
            db_conn.close()
            return_data=bytes("Error",'utf8')
            size=len(return_data).to_bytes(4,byteorder='big')
            return [size,return_data]

        db_conn.close()

    

    return_data=bytes("Success",'utf8')
    size=len(return_data).to_bytes(4,byteorder='big')
    return [size,return_data]

def share_location(data):

    pool = psycopg2.connect(database="SafePlace", user="postgres", password="1091102", host="127.0.0.1", port="5432")
    with pool.cursor() as db_conn:
        
        id=data[1]
        

        # print(data)

        if id=='0':
            hotp=pyotp.HOTP(pyotp.random_base32())
            
            otp1=hotp.at(1108)
            otp2=hotp.at(628)
            otp3=hotp.at(1102)

            rel=db_conn.execute("SELECT max(id) FROM user_position")
            rel=db_conn.fetchall()
            if rel[0][0]==None:
                id=1
            else:
                id=rel[0][0]+1

            db_conn.execute(f"INSERT INTO otp_position (otp,id) VALUES('{otp1}',{id})")
            db_conn.execute(f"INSERT INTO otp_position (otp,id) VALUES('{otp2}',{id})")
            db_conn.execute(f"INSERT INTO otp_position (otp,id) VALUES('{otp3}',{id})")
            pool.commit()
            return_data=str(id)+" "+otp1+" "+otp2+" "+otp3
        else:
            pos_secret=data[2]
            time=data[3]
            try:

                db_conn.execute(f"INSERT INTO user_position (id,position,time) VALUES('{id}','{pos_secret}','{time}')")
                pool.commit()
                return_data="Success"
                
            except:
                db_conn.close()
                return_data=bytes("Error",'utf8')
                size=len(return_data).to_bytes(4,byteorder='big')
                return [size,return_data]



        db_conn.close()

    

    return_data=bytes(return_data,'utf8')
    size=len(return_data).to_bytes(4,byteorder='big')
    return [size,return_data]

def verify_otp(data):
    pool = psycopg2.connect(database="SafePlace", user="postgres", password="1091102", host="127.0.0.1", port="5432")
    with pool.cursor() as db_conn:
        otp=data[1]

        try:
            rel=db_conn.execute(f"SELECT id FROM otp_position WHERE otp='{otp}'")
            rel=db_conn.fetchall()
            if(rel==None):
                return_data="Error"
            else:
                return_data=str(rel[0][0]*1024-511)
                # db_conn.execute(f"DELETE FROM otp_position WHERE otp='{otp}'")
                pool.commit()
        except:
            db_conn.close()
            return_data=bytes("Error",'utf8')
            size=len(return_data).to_bytes(4,byteorder='big')           
            return [size,return_data]

    db_conn.close()

    return_data=bytes(return_data,'utf8')
    size=len(return_data).to_bytes(4,byteorder='big')
    return [size,return_data]


def get_location(data):
    

    pool = psycopg2.connect(database="SafePlace", user="postgres", password="1091102", host="127.0.0.1", port="5432")
    with pool.cursor() as db_conn:

        try:
            id=int((int(data[1])+511)/1024)
            time=data[2]
            # print(id)
            db_conn.execute(f"SELECT * FROM user_position WHERE id={id}")
            rel=db_conn.fetchall()
            # print(rel)
            
            if rel==[]:
                return_data="Error"
            else:
                db_conn.execute(f"SELECT position,time FROM user_position WHERE id={id} AND (time>'{time}' OR ('{time}'>'23:00:00+8' AND time<'01:00:00+8')) ORDER BY time ASC")
                rel=db_conn.fetchall()
                return_data=""

                
                if rel==[]:
                    return_data="NO DATA"
                for row in rel:
                    # print(row)
                    return_data+=row[0]+" "+str(row[1])+"\n"
                    
                    

        except:
            db_conn.close()
            return_data=bytes("Error",'utf8')
            size=len(return_data).to_bytes(4,byteorder='big')
            return [size,return_data]

    db_conn.close()

    return_data=bytes(return_data,'utf8')
    size=len(return_data).to_bytes(4,byteorder='big')
    return [size,return_data]




class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


def client(ip, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((ip, port))
        
        
        sock.sendall(bytes(message, 'utf8'))


        data_size =  int.from_bytes(sock.recv(4), byteorder='big')

        data=bytearray()

        #print(data_size)
        
        while(len(data)!=data_size):
            
            response=sock.recv(1024)
            #print(response)
            
            data.extend(response)
            #byte_li+=li
            #print(len(data))
 
        data=str(data.decode('utf8'))
        

        print("Received: {}".format(data))
           

import argparse
    

if __name__ == "__main__":




    HOST, PORT = '', 8888
    print(PORT)
    server = ThreadedTCPServer((HOST, PORT), SafePlaceThreadedTCPServerHandler)

    with server:
        ip, port = server.server_address

        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        print("Server loop running in thread:", server_thread.name)

        
        # client(ip,port,"NEWST|food panda|food|Great|rgkprjg|0|23")
        # client(ip,port,"NEWST|213|Convience|123|123|4|5")
        # client("35.206.214.161",port,"UPDATE|v0")
        client(ip,port,"DOWNLOAD|v0")
        # client(ip,port,"REPORT|11|NOT EXIT")
        # client(ip,port,"SHARE|0|77777|03:00:00+8")
        # client(ip,port,"GETLOC|2561|00:00:00+8")
        # client(ip,port,"VYOTP|434315")
        # client_thread1=threading.Thread(target=client,args=[ip,port,"DOWNLOAD|v0"])
        # client_thread2=threading.Thread(target=client,args=[ip,port,"DOWNLOAD|v0"])
        # client_thread1.start()
        # client_thread2.start()

        # print("end")
     

        # server.shutdown()


        # while(1):

        #     pool = sqlalchemy.create_engine("postgresql+pg8000://",creator=getconn)
        #     with pool.connect() as db_conn:

        #         cur_time=datetime.now(timezone.utc)-timedelta(minutes=30)
        #         print(str(cur_time.astimezone().timetz()))
        #         rel=db_conn.execute(f"SELECT id FROM user_position WHERE time<'{cur_time}' ORDER BY id ASC").fetchall()

        #         id="0"
        #         for row in rel:
                    
        #             if id!=row[0]:
        #                 id=row[0]
        #                 db_conn.execute(f"DELETE FROM user_position WHERE id={id}")
        #                 db_conn.execute(f"DELETE FROM otp_position WHERE id={id}")
        #                 print("Delete "+str(id))
                    
        #             # if id!=row[0] and cur_time-row[2]>:

        #     db_conn.close()
                    

                

        #     time.sleep(60)

        server_thread.join()
        # server.shutdown()