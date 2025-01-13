import tkinter as tk
import sys
import socket
import socketserver as SocketServer

import os
import select
import getopt
import time

import paramiko
import threading
import queue

from urllib.parse import urlparse, parse_qs
import yaml

HOST = "127.0.0.1"
PORT = 65432
SSH_PORT = 22
REMOTE_CONFIG_FILE = 'remote_config.yaml'

SSH_CONNECT_ERROR = 101
SSH_TUNNEL_ERROR = 102
SSH_NOPORT_ERROR = 103
SSH_NO_ERROR = 0

SUPPORTED_PROTOCOLS = ['ssh', 'ssh_tunnel']
SUPPORTED_AUTH_METHODS = ['passcode', 'password']
THREAD_CHECK_TIME = 0.5  # 0.5 seconds

class LoginWindowRemote(object):
    def __init__(self, master, remote, username, auth):
        global g_remote_user, g_remote_pass
        g_remote_user = None
        g_remote_pass = None
        self.master = master
        
        # Remote host
        self.rl = tk.Label(master, text="Remote Host: " + remote)
        self.rl.config(font=("Courier", 14))
        self.rl.pack()
        self.rl1 = tk.Label(master, text="Username")
        self.rl1.config(font=("Courier", 14))
        self.rl1.pack()
        self.rb = tk.Entry(master, font=('Century 12'), width=64)
        if username is not None:
            self.rb.insert(0,username)
        self.rb.pack()
        self.rl1 = tk.Label(master, text=auth)
        self.rl1.config(font=("Courier", 14))
        self.rl1.pack()
        self.rb1 = tk.Entry(master, show="*", font=('Century 12'), width=64)
        self.rb1.pack()

        master.bind('<Return>', self.get_value)
        self.b2 = tk.Button(master, text="Enter", command=self.get_value)
        self.b2.pack()

    def get_value(self, event=None):
        global g_remote_user, g_remote_pass
        g_remote_user = self.rb.get()
        g_remote_pass = self.rb1.get()
        # print("g_user:",g_user,g_pass)
        self.destroy()

    def destroy(self):
        print("Destroying window.")
        self.master.withdraw()
        self.master.quit()
        self.master.destroy()
        print("Done!")

class LoginWindowJumpRemote(object):
    def __init__(self, master,
                 jump_host, jump_user, jump_auth,
                 remote_host, remote_user, remote_auth):
        global g_remote_user, g_remote_pass
        global g_jump_user, g_jump_pass
        g_remote_user = None
        g_remote_pass = None
        g_jump_user   = None
        g_jump_pass   = None
        self.master = master

        # Jump host
        self.jl = tk.Label(master, text="Jump Host: " + jump_host)
        self.jl.config(font=("Courier", 14))
        self.jl.pack()
        self.jl1 = tk.Label(master, text="Jump Username")
        self.jl1.config(font=("Courier", 14))
        self.jl1.pack()
        self.jb = tk.Entry(master, font=('Century 12'), width=64)
        if jump_user is not None:
            self.jb.insert(0,jump_user)
        self.jb.pack()
        self.jl1 = tk.Label(master, text=jump_auth)
        self.jl1.config(font=("Courier", 14))
        self.jl1.pack()
        self.jb1 = tk.Entry(master, show="*", font=('Century 12'), width=64)
        self.jb1.pack()

        # Remote host
        self.rl = tk.Label(master, text="Remote Host: " + remote_host)
        self.rl.config(font=("Courier", 14))
        self.rl.pack()
        self.rl1 = tk.Label(master, text="Remote Username")
        self.rl1.config(font=("Courier", 14))
        self.rl1.pack()
        self.rb = tk.Entry(master, font=('Century 12'), width=64)
        if remote_user is not None:
            self.rb.insert(0,remote_user)
        self.rb.pack()
        self.rl1 = tk.Label(master, text=remote_auth)
        self.rl1.config(font=("Courier", 14))
        self.rl1.pack()
        self.rb1 = tk.Entry(master, show="*", font=('Century 12'), width=64)
        self.rb1.pack()

        master.bind('<Return>', self.get_value)
        self.b2 = tk.Button(master, text="Enter", command=self.get_value)
        self.b2.pack()

    def get_value(self, event=None):
        global g_remote_user, g_remote_pass
        global g_jump_user, g_jump_pass
        g_jump_user   = self.jb.get()
        g_jump_pass   = self.jb1.get()
        g_remote_user = self.rb.get()
        g_remote_pass = self.rb1.get()
        # print("g_user:",g_user,g_pass)
        self.destroy()

    def destroy(self):
        print("Destroying window.")
        self.master.withdraw()
        self.master.quit()
        self.master.destroy()
        print("Done!")

g_queue = queue.Queue()

# BEGIN
# From forward.py in paramiko
# https://github.com/paramiko/paramiko/blob/main/demos/forward.py
#
g_verbose = True
def verbose(s):
    global g_verbose
    if g_verbose:
        print(s)

class ForwardServer(SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

class Handler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            chan = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.chain_host, self.chain_port),
                self.request.getpeername(),
            )
        except Exception as e:
            verbose(
                "Incoming request to %s:%d failed: %s"
                % (self.chain_host, self.chain_port, repr(e))
            )
            return
        if chan is None:
            verbose(
                "Incoming request to %s:%d was rejected by the SSH server."
                % (self.chain_host, self.chain_port)
            )
            return

        verbose(
            "Connected!  Tunnel open %r -> %r -> %r"
            % (
                self.request.getpeername(),
                chan.getpeername(),
                (self.chain_host, self.chain_port),
            )
        )
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                   break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                   break
                self.request.send(data)

        peername = self.request.getpeername()
        chan.close()
        self.request.close()
        verbose("Tunnel closed from %r" % (peername,))
        print("SELF TEMP_TUNNEL: ",self.temp_tunnel)
        if self.temp_tunnel == True:
            g_queue.put((self.chain_host,self.chain_port))
#
# From forward.py in paramiko
# END

#
# SSH tunnel implementation based on forward.py in paramiko
#
class SSHOptions():
    def __init__(self, keyfile=None, look_for_keys=True):
        self.keyfile = keyfile
        self.look_for_keys = look_for_keys
    
    def set_options(self, keyfile=None, look_for_keys=True):
        self.keyfile = keyfile
        self.look_for_keys = look_for_keys

class SSHServerInfo():
    def __init__(self, host_name, host_port):
        self.host_name = host_name
        self.host_port = host_port
    
    def set_server(self, host_name, host_port):
        self.host_name = host_name
        self.host_port = host_port

class SSHUserInfo():
    def __init__(self, user_name, user_pass):
        self.user_name = user_name
        self.user_pass = user_pass
    
    def set_user(self, user_name, user_pass):
        self.user_name = user_name
        self.user_pass = user_pass

class SSHJumpLinkInfo():
    def __init__(self,transport,channel):
        self.transport = transport
        self.channel   = channel
    
    def set_jump_link(self,transport,channel):
        self.transport = transport
        self.channel   = channel
        
class SSHConnectedServerInfo():
    def __init__(self, server: SSHServerInfo, user: SSHUserInfo, client: paramiko.SSHClient):
        self.server = server
        self.user   = user
        self.client = client
    
    def set_server_info(self,server: SSHServerInfo, user: SSHUserInfo, client: paramiko.SSHClient):
        self.server = server
        self.user   = user
        self.client = client
    
class SSHRemoteConnectionInfo():
    def __init__(self, remote: SSHConnectedServerInfo, jump: SSHConnectedServerInfo, jump_link: SSHJumpLinkInfo):
        self.remote    = remote
        self.jump      = jump
        self.jump_link = jump_link
    
    def set_remote_connection_info(self, remote: SSHConnectedServerInfo, jump: SSHConnectedServerInfo, jump_link: SSHJumpLinkInfo):
        self.remote    = remote
        self.jump      = jump
        self.jump_link = jump_link

class SSHTunnelInfo():
    def __init__(self,
                ssh_client=None, service_group=None, service_name=None,
                jump_host=None,
                remote_host=None, remote_port=None, local_port=None,
                cookie=None, thread_process=None, 
                forward_server=None, forward_tunnel=False, 
                dest_host=None, dest_port=None, reverse_tunnel=False):
        self.ssh_client     = ssh_client
        self.service_group  = service_group
        self.service_name   = service_name
        self.jump_host      = jump_host
        self.remote_host    = remote_host
        self.remote_port    = remote_port
        self.dest_host      = dest_host
        self.dest_port      = dest_port
        self.local_port     = local_port
        self.cookie         = cookie
        self.process        = thread_process
        self.forward_server = forward_server
        self.forward_tunnel = forward_tunnel

        self.reverse_tunnel = reverse_tunnel

        if self.forward_server != None:
            self.forward_tunnel = True
    
    def set_tunnel_info(self,                
                        ssh_client=None, service_group=None, service_name=None,
                        jump_host=None,
                        remote_host=None, remote_port=None, local_port=None,
                        cookie=None, thread_process=None, 
                        forward_server=None, forward_tunnel=False, 
                        dest_host=None, dest_port=None, reverse_tunnel=False):
        self.ssh_client     = ssh_client
        self.service_group  = service_group
        self.service_name   = service_name
        self.jump_host      = jump_host
        self.remote_host    = remote_host
        self.remote_port    = remote_port
        self.dest_host      = dest_host
        self.dest_port      = dest_port
        self.local_port     = local_port
        self.cookie         = cookie
        self.process        = thread_process
        self.forward_server = forward_server
        self.forward_tunnel = forward_tunnel
        self.reverse_tunnel = reverse_tunnel

        if self.forward_server != None:
            self.forward_tunnel = True
                                      
class SSHConnectRemote():
    def __init__(self):
        self.options = SSHOptions()
        self.jump = None
        self.jump_transport = None
        self.jump_channel   = None
        self.remote = None
        self.remote_connection_info = None

    def _connect_server(self,
                        host_name: str,
                        ssh_port:  int = SSH_PORT,
                        user_name: str = None,
                        user_pass: str = None,
                        sock_channel: paramiko.Channel = None):        
        verbose("Connecting to remote server %s:%d ..." % (host_name,ssh_port))

        # Connect to jump server
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(hostname=host_name,
                            port=ssh_port,
                            username=user_name,
                            key_filename=self.options.keyfile,
                            look_for_keys=self.options.look_for_keys,
                            password=user_pass,
                            sock=sock_channel)
        except Exception as e:
            print("*** Failed to connect to %s:%d: %r" % (host_name, ssh_port, e))
            return SSH_CONNECT_ERROR, None
        
        server = SSHServerInfo(host_name,ssh_port)
        user   = SSHUserInfo(user_name,user_pass)
        remote = SSHConnectedServerInfo(server,user,client)

        return SSH_NO_ERROR, remote

    def connect_remote(self,
                        host_name: str = None,
                        ssh_port:  int = SSH_PORT,
                        user_name: str = None,
                        user_pass: str = None,
                        sock_channel: paramiko.Channel = None):        

        conn_err, self.remote = self._connect_server(host_name,ssh_port,user_name,user_pass,sock_channel=sock_channel)
        if conn_err != SSH_NO_ERROR:
            return SSH_CONNECT_ERROR
        
        self.remote_connection_info = SSHRemoteConnectionInfo(self.remote,jump=None,jump_link=None)
        return SSH_NO_ERROR

    def connect_remote_via_jump(self,
                                connected_jump: SSHConnectedServerInfo,
                                remote_host_name: str, 
                                remote_ssh_port:  int,
                                remote_user_name: str, 
                                remote_user_pass: int):

        verbose("Connecting to remote server %s via connected jump server %s." %
                (remote_host_name, connected_jump.server.host_name))

        try:
            # Create a channel via jump server
            self.jump_transport = connected_jump.client.get_transport()
            dest_addr = (remote_host_name,remote_ssh_port)
            src_addr  = (connected_jump.server.host_name, connected_jump.server.host_port)
            self.jump_channel = self.jump_transport.open_channel("direct-tcpip", dest_addr, src_addr)
        except Exception as e:
            print("An exception has occured: %r" % (e))
            return None


        # Connect to remote server (destination for port forwarding)
        conn_err, self.remote = self._connect_server(host_name=remote_host_name,
                                                    ssh_port=remote_ssh_port,
                                                    user_name=remote_user_name,
                                                    user_pass=remote_user_pass,
                                                    sock_channel=self.jump_channel)
        if conn_err != SSH_NO_ERROR:
            return SSH_CONNECT_ERROR
        
        self.jump = connected_jump
        jump_link = SSHJumpLinkInfo(self.jump_transport,self.jump_channel)
        self.remote_connection_info = SSHRemoteConnectionInfo(self.remote,self.jump,jump_link)

        return SSH_NO_ERROR
         
    def get_remote_connection_info(self):
        return self.remote_connection_info
      
class SSHLocalRemoteTunnel():
    def __init__(self, remote_connection: SSHConnectRemote = None):
        # Connected remote server for local or reverse port forwarding
        self.remote_connection      = remote_connection 
        self.remote_connection_info = remote_connection.get_remote_connection_info()
        # Port forwarding servers
        self.forward_server = None
        self.local_port     = None

    def set_remote_connection(self, remote_connection: SSHConnectRemote):
        self.remote_connection = remote_connection
        self.remote_connection_info = remote_connection.get_remote_connection_info()
    
    def get_remote_connection(self):
        return self.remote_connection
    
    def get_remote_connection_info(self):
        return self.remote_connection_info
          
    def exec_remote_command(self, service_command: str):
        client = self.remote_connection_info.remote.client
        return client.exec_command(service_command, get_pty=False)

    def _forward_tunnel(self, local_port: int, dest_host: str, dest_port: int, transport: paramiko.Transport, temptunnel: bool):
        class SubHandler(Handler):
            chain_host    = dest_host
            chain_port    = dest_port
            ssh_transport = transport
            temp_tunnel   = temptunnel

        forward_server = ForwardServer(("", local_port), SubHandler)
        return forward_server

    def _ssh_forward(self, client: paramiko.SSHClient, local_port: int, dest: SSHServerInfo, temptunnel: bool):
        verbose("Opening tunnel for local port %d to %s:%d " 
                % (local_port, dest.host_name, dest.host_port))
        forward_server = None
        try:
            forward_server = self._forward_tunnel(
                local_port, dest.host_name, dest.host_port, client.get_transport(),temptunnel)
            return forward_server
        except Exception as e:
            print("An exception has occured: %r" % (e))
            return None

    def _run_forward_server(self, forward_server):
        verbose("Starting.")
        forward_server.serve_forever()
        verbose("Shut down.")

    def start_forward_server(self, local_port: int, dest_server: SSHServerInfo, temptunnel: bool):
        client = self.remote_connection_info.remote.client
        self.forward_server = self._ssh_forward(client,local_port,dest_server,temptunnel)

        if self.forward_server is None:
            return None, None, None, SSH_TUNNEL_ERROR
        verbose("Got the forward server")
            
        thread = threading.Thread(target=self._run_forward_server,args=(self.forward_server,))
        thread.daemon = True
        thread.start()
        while thread.is_alive() == False:
            time.sleep(THREAD_CHECK_TIME)

        return self.forward_server, thread, SSH_NO_ERROR

    def _reverse_forward_handler(self, chan: paramiko.Channel, host: str, port: int):
        sock = socket.socket()
        try:
            sock.connect((host, port))
        except Exception as e:
            verbose("Forwarding request to %s:%d failed: %r" % (host, port, e))
            return

        verbose("Connected!  Tunnel open %r -> %r -> %r"
            % (chan.origin_addr, chan.getpeername(), (host, port)))
        
        while True:
            r, w, x = select.select([sock, chan], [], [])
            if sock in r:
                data = sock.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                sock.send(data)
        chan.close()
        sock.close()
        verbose("Tunnel closed from %r" % (chan.origin_addr,))

    def _reverse_forward_tunnel(self, remote_server_port: int, dest_host: str, dest_port: int):
        client = self.remote_connection_info.remote.client
        transport = client.get_transport()
        transport.request_port_forward("", remote_server_port)
        while True:
            chan = transport.accept(1000)
            if chan is None:
                continue
            thread = threading.Thread(
                target=self._reverse_forward_handler, args=(chan, dest_host, dest_port)
            )
            thread.daemon = True
            thread.start()
            while thread.is_alive() == False:
                time.sleep(THREAD_CHECK_TIME)

    def reverse_forward_tunnel(self, remote_server_port: int, dest_host: str, dest_port: int):
        thread = threading.Thread(
            target=self._reverse_forward_tunnel, args=(remote_server_port, dest_host, dest_port)
        )
        thread.deamon = True
        thread.start()
        while thread.is_alive() == False:
            time.sleep(THREAD_CHECK_TIME)
        return thread

    def shutdown_forward_server(self):
        verbose('Shutting down tunnel.')
        self.forward_server.shutdown()

g_server_config_data = None
def read_yaml_config(filename: str):
    config_data = None
    if filename != None:
       f = open(filename, mode='r')
       config_data = yaml.safe_load(f)
       f.close()
    return config_data

g_open_tunnel_list : SSHTunnelInfo = []
g_remote_conn_list : SSHConnectRemote = []
class MyTCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        global g_server_config_data
        global g_open_tunnel_list
        
        # self.server_config_data = g_server_config_data
        req_data = "".join(self.request.recv(2048).decode("UTF-8").split())
        req_data = "".join(req_data.rsplit("\x00"))
        print("Client {}:".format(self.client_address[0]))
        print("Request  :", req_data)

        req_path, req_qry = self.parse_request(req_data)
        if not req_qry:
            self.send_response("port:-1,msg:incorrect_request_format")
            return
        print("Parsed Request: ",req_qry)

        if req_path == "/run_service":
            self.do_run_remote_service(req_qry)
            return
        elif req_path == "/connect_port": 
            # /connect_port?jhost=<jumphost>&juser=<jumpuser>&rhost=<remotehost>&ruser=<remoteuser>&dhost=<desthost>&dport=<destport>
            # /connect_port?group=<group_name>&service=<service_name>&dhost=<desthost>&dport=<destport>
            self.do_connect_port(req_qry)
        elif req_path == "/reverse":
            self.do_reverse_port_forward(req_qry)
            return
        else:
            self.send_response("port:-1,msg:incorrect_request_format")
            return

    def parse_request(self,req_data):
        req_tmp = urlparse(req_data)
        req_qry = parse_qs(req_tmp.query)
        return req_tmp.path, req_qry

    def send_response(self, res_str):
        self.request.sendall(res_str.encode("UTF-8"))
        return

    def login_window_remote(self, try_cnt: int,
                            remote_host_name: str,
                            remote_user_name: str = None,
                            auth_type: str = 'Password'):
        global g_remote_user, g_remote_pass
        root = tk.Tk()
        tmp_str = "Login Window Remote"
        if try_cnt !=0:
            tmp_str = "Failed ("+str(try_cnt)+"/3). Try Again."
        root.title(tmp_str)
        _ = LoginWindowRemote(root,remote_host_name,remote_user_name,auth_type)
        root.mainloop()
        remote_user = SSHUserInfo(g_remote_user,g_remote_pass)
        return remote_user
    
    def login_window_jump_remote(self, try_cnt:   int,
                                jump_host_name:   str, 
                                remote_host_name: str,
                                jump_user_name:   str = None, 
                                remote_user_name: str = None,
                                jump_auth: str   = 'Password', 
                                remote_auth: str = 'Password'):
        global g_remote_user, g_remote_pass
        global g_jump_user, g_jump_pass

        root = tk.Tk()
        tmp_str = "Login Window Jump Remote"
        if try_cnt !=0:
            tmp_str = "Failed ("+str(try_cnt)+"/3). Try Again."
        root.title(tmp_str)
        _ = LoginWindowJumpRemote(root,
                                  jump_host_name,jump_user_name,jump_auth,
                                  remote_host_name,remote_user_name,remote_auth)
        root.mainloop()
        jump_user   = SSHUserInfo(g_jump_user,g_jump_pass)
        remote_user = SSHUserInfo(g_remote_user,g_remote_pass)
        return jump_user, remote_user
    
    def check_connected_remote(self, remote_host_name: str, remote_user_name: str) -> SSHConnectRemote:
        global g_remote_conn_list
        for srvr in g_remote_conn_list:
            srvr_info = srvr.get_remote_connection_info()
            verbose("Checking if %s %s exists: %s %s" % (
                remote_host_name, remote_user_name, 
                srvr_info.remote.server.host_name, 
                srvr_info.remote.user.user_name      
            ))
            if ((srvr_info.remote.server.host_name == remote_host_name) and
                (srvr_info.remote.user.user_name == remote_user_name)):
                return srvr 
        return None
    
    def login_connect_remote(self,req_def) -> SSHConnectRemote:
        remote_host_name = req_def['remote_host']
        remote_user_name = req_def['username']
        ssh_connected_remote = self.check_connected_remote(remote_host_name,remote_user_name)
        if ssh_connected_remote != None:
            return ssh_connected_remote
        
        jump_host_name = req_def['jumphost']
        jump_user_name = req_def['jumpuser']
        ssh_connected_jump = None
        if jump_host_name != None:
            ssh_connected_jump = self.check_connected_remote(jump_host_name,jump_user_name)
                
        try_connect_cnt = 0
        ssh_connect_remote = None
        ssh_connect_jump   = None
        while try_connect_cnt <=3:
            ssh_connect_remote = SSHConnectRemote()
            if ssh_connected_jump != None:
                remote_user = self.login_window_remote(try_connect_cnt,
                                                        remote_host_name,
                                                        remote_user_name,
                                                        req_def['auth'].capitalize())
                if remote_user.user_name is None:
                    ssh_connected = False
                    break
                jump_info = ssh_connected_jump.get_remote_connection_info()
                error_no  = ssh_connect_remote.connect_remote_via_jump(connected_jump=jump_info.remote,
                                                                    remote_host_name=remote_host_name,
                                                                    remote_ssh_port=SSH_PORT,
                                                                    remote_user_name=remote_user.user_name,
                                                                    remote_user_pass=remote_user.user_pass)    
            elif jump_host_name != None:
                ssh_connect_jump = SSHConnectRemote()
                jump_user, remote_user = self.login_window_jump_remote(try_connect_cnt,
                                                                        jump_host_name,
                                                                        remote_host_name,
                                                                        jump_user_name,
                                                                        remote_user_name,
                                                                        req_def['auth'].capitalize(),
                                                                        req_def['auth'].capitalize())
                if remote_user.user_name is None:
                    ssh_connected = False
                    break
                # Connect to the jump host
                error_no = ssh_connect_jump.connect_remote(host_name=jump_host_name,
                                                            ssh_port=SSH_PORT,
                                                            user_name=jump_user.user_name,
                                                            user_pass=jump_user.user_pass,
                                                            sock_channel=None)
                if error_no != SSH_NO_ERROR:
                    ssh_connected = False
                    break
                # Connect to remote via connected jump host
                error_no = ssh_connect_remote.connect_remote_via_jump(connected_jump=ssh_connect_jump.remote,
                                                                    remote_host_name=remote_host_name,
                                                                    remote_ssh_port=SSH_PORT,
                                                                    remote_user_name=remote_user.user_name,
                                                                    remote_user_pass=remote_user.user_pass)    
            else:
                remote_user = self.login_window_remote(try_connect_cnt,
                                                        remote_host_name,
                                                        remote_user_name,
                                                        req_def['auth'].capitalize())
                if remote_user.user_name is None:
                    ssh_connected = False
                    break
                error_no = ssh_connect_remote.connect_remote(host_name=remote_host_name,
                                                            ssh_port=SSH_PORT,
                                                            user_name=remote_user.user_name,
                                                            user_pass=remote_user.user_pass,
                                                            sock_channel=None)    
                
            if error_no == SSH_NO_ERROR:
                ssh_connected = True
                break
            try_connect_cnt = try_connect_cnt + 1

        if ssh_connected == True:
            g_remote_conn_list.append(ssh_connect_remote)
            if ssh_connect_jump != None:
                g_remote_conn_list.append(ssh_connect_jump)
            return ssh_connect_remote
        else:
            return None
    
    def check_requested_local_port_available(self, requested_port, max_count=200):
        verbose("Checking if port %d is available." % (requested_port))
        result  = False
        try_cnt = 0
        while (result == False) and (try_cnt < max_count):
            a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            location = ("127.0.0.1", requested_port)
            try:
                a_socket.bind(location)
                result = True
            except:
                result = False
                try_cnt = try_cnt + 1
            a_socket.close()

        if result == False:
            verbose("Port %s is not available after %d tries" % (requested_port, self.max_count))
            return False
        else:
            return True

    def find_any_available_local_port(self, port_start=28000, max_count=2000):
        a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        location = ("", 0)
        assigned_port = None
        host_address = None
        try:
            a_socket.bind(location)
            (host_address,assigned_port) = a_socket.getsockname()
            a_socket.close()
        except:
            verbose("Couldn't find any open ports")
            return None
        return assigned_port

    def find_available_local_port(self, port_start=28000, max_count=2000):
        result  = False
        try_cnt = 0
        assigned_port = port_start
        while (result == False) and (try_cnt < max_count):
            verbose("Checking if port %d is available." % (assigned_port))
            a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            location = ("127.0.0.1", assigned_port)
            try:
                a_socket.bind(location)
                result = True
            except:
                result = False
                assigned_port = assigned_port + 1
                try_cnt = try_cnt + 1
            a_socket.close()

        if result == False:
            verbose("Couldn't find any open ports after %d tries" % (max_count))
            return None
        else:
            return assigned_port
      
    def get_local_port(self, requested_port=None, max_count=2000):
        self.local_port = requested_port
        if requested_port is not None:
            if self.check_requested_local_port_available(requested_port=requested_port,max_count=max_count) == False:
                self.local_port = None
        else:
            self.local_port = self.find_available_local_port()
        return self.local_port

    def check_service_running(self, service_name, service_group):
        global g_open_tunnel_list
        for tnnl in g_open_tunnel_list:
            if ((service_name == tnnl.service_name) and 
                (service_group == tnnl.service_group)):
                if tnnl.cookie != None:
                    return tnnl.local_port,tnnl.cookie 
                return tnnl.local_port,None
        return None,None

    def parse_service_request(self,req_qry):
        if 'version' not in g_server_config_data:
            verbose("No config version, assuming v0.1")
            return self.parse_service_request_version01(req_qry)
        if g_server_config_data['version'] == 'v0.1':
            verbose("Config version v0.1")
            return self.parse_service_request_version01(req_qry)
        verbose("Unsupported configuration version: %s" % (g_server_config_data['version'],))
        return None

    def parse_service_request_version01(self,req_qry):
        req_group_name   = req_qry['group'][0]
        req_service_name = req_qry['service'][0]
        for group_name in g_server_config_data:
            if group_name == req_group_name:
                service_group = g_server_config_data[group_name]
                for service_name in service_group:
                    if service_name == req_service_name:
                        service_data = service_group[service_name]
                        if (('serverpath' not in service_data) or
                            ('host' not in service_data) or
                            ('protocol' not in service_data) or
                            ('authentication' not in service_data)):
                            return None
                        req_def = {}
                        req_def['service_group'] = req_group_name
                        req_def['service_name']  = service_name
                        req_def['service_prog']  = service_data['serverpath']
                        req_def['service_args']  = None
                        if 'args' in service_data:
                            req_def['service_args'] = service_data['args']
                        req_def['username'] = None
                        if 'user' in service_data:
                            req_def['username'] = service_data['user']
                        req_def['remote_host'] = service_data['host']
                        req_def['dest_host'] = service_data['host']
                        req_def['jumphost'] = None
                        if 'jumphost' in service_data:
                            req_def['jumphost'] = service_data['jumphost']
                        req_def['jumpuser'] = None
                        if 'jumpuser' in service_data:
                            req_def['jumpuser'] = service_data['jumpuser']
                        if 'port' in service_data:
                            req_def['remote_port'] = int(service_data['port'])
                        else:
                            req_def['remote_port'] = -1
                        req_def['tunneltype'] = "permanent"
                        if ('tunneltype' in service_data):
                            req_def['tunneltype'] = service_data['tunneltype']
                        req_def['conn'] = service_data['protocol']
                        req_def['auth'] = service_data['authentication']
                        if (('local_port' not in req_qry) and
                            ('local_port' in service_data)):
                            req_def['local_port'] = service_data['local_port']
                        elif (('local_port' in req_qry) and
                            (int(req_qry['local_port'][0]) > 0)):
                            req_def['local_port'] = req_qry['local_port'][0]
                        return req_def
        return None

    def do_run_remote_service(self, req_qry):
        print("Remote service request: ", req_qry)
        if 'group' not in req_qry:
            self.send_response("port:-1,msg:missing_group_in_request")
            return
        if 'service' not in req_qry:
            self.send_response("port:-1,msg:missing_service_in_request")
            return

        local_port, service_cookie = self.check_service_running(service_name=req_qry['service'][0],
                                                             service_group=req_qry['group'][0])
        if local_port != None:
            if service_cookie != None:
                self.send_response("port:"+str(local_port)+",cookie:"+str(service_cookie)+",msg:no_error")
            else:
                self.send_response("port:"+str(local_port)+",msg:no_error")
            return

        req_def = self.parse_service_request(req_qry)
        if req_def == None:
            self.send_response("port:-1,msg:incomplete_service_definition")
            return

        print("Service request: ", req_def)
        self.run_service(req_def)
        return

    def parse_service_response(self,r_stdout,r_stderr):
        print("Parsing service response...")
        service_data = {}
        # for line in r_stdout.readlines():  -- replaced it with the following
        # two lines. Also commented out the break statement.
        line = r_stdout.readline()
        if line != '':
            print("LINE: ", line)
            srvr_line = line.replace(" ","")
            srvr_line = srvr_line.replace("\r","")
            srvr_line = srvr_line.replace("\n","")
            if ("port:" in srvr_line) and ("msg:" in srvr_line):
                srv_str = srvr_line.replace(" ", "")
                srv_str = srv_str.replace("\n", "")
                srv_str = srv_str.split(';')
                for srv_item in srv_str:
                    tmp_str = srv_item.split(':', maxsplit=1)
                    service_data[tmp_str[0]] = tmp_str[1]
                # break
        if not service_data:
            # for line in r_stderr.readline(): -- replaced it with the
            # following two lines. Also commented out the break statement. 
            line = r_stderr.readline()
            if line != '': # for line in r_stderr.readline():
                print("LINE: ", line)
                srvr_line = line.replace(" ", "")
                srvr_line = srvr_line.replace("\r","")
                srvr_line = srvr_line.replace("\n","")
                if ("port:" in srvr_line) and ("msg:" in srvr_line):
                    srv_str = srvr_line.replace(" ", "")
                    srv_str = srv_str.replace("\n", "")
                    srv_str = srv_str.split(';')
                    for srv_item in srv_str:
                        tmp_str = srv_item.split(':', maxsplit=1)
                        service_data[tmp_str[0]] = tmp_str[1]
                    # break
        if not service_data:
            return None
        if 'cookie' not in service_data:
            service_data['cookie'] = None
        return service_data

    def run_service(self, req_def):
        global g_open_tunnel_list
        print("Run Service Request:", req_def)

        if (('service_prog' not in req_def) or
            ('remote_host' not in req_def) or
            ('conn' not in req_def) or
            ('auth' not in req_def)):
            self.send_response("port:-1,msg:incorrect_service_request_format")
            return
        if (not req_def['conn'] in SUPPORTED_PROTOCOLS):
            self.send_response("port:-1,msg:unsupported_protocol")
            return
        if (not req_def['auth'] in SUPPORTED_AUTH_METHODS):
            self.send_response("port:-1,msg:unsupported_authication_method")
            return
        
        requested_port = None
        if 'local_port' in req_def:
            requested_port = int(req_def['local_port'])
            if self.check_requested_local_port_available(requested_port) ==False:
                self.send_response("port:"+str(requested_port)+",msg:requested_port_is_not_available")
                return

        ssh_connect_remote = self.login_connect_remote(req_def)
        if ssh_connect_remote == None:
            self.send_response("port:-1,msg:cannot_connect_to_remote_server")
            return

        ssh_tunnel_client = SSHLocalRemoteTunnel(ssh_connect_remote)

        # Run the remote service
        service_command = req_def['service_prog']
        if req_def['service_args'] != None:
            service_command = service_command + " " + req_def['service_args']
        print("Service command:",service_command)
        r_stdin, r_stdout, r_stderr = ssh_tunnel_client.exec_remote_command(service_command)
        service_data = self.parse_service_response(r_stdout,r_stderr)
        print("SERVICE DATA: ",service_data)
        if service_data == None:
            self.send_response("port:-1,msg:service_not_available")
            return
        print("Service data:", service_data)
        service_data['port'] = int(service_data['port'])
        if service_data['port']<0:
            self.send_response("port:-1,msg:cannot_find_service_port_on_remote")
            return

        service_name  = req_def['service_name']
        service_group = req_def['service_group']
        remote_host   = req_def['remote_host']
        remote_port   = service_data['port']
        dest_host     = req_def['dest_host']
        dest_port     = service_data['port']
        remote_srvr   = SSHServerInfo(dest_host,dest_port)

        local_port = self.get_local_port(requested_port)
        if local_port == None:
            self.send_response("port:-1,msg:cannot_find_local_port")
            return
        
        temptunnel = False
        if "transient" in req_def['tunneltype']:
            temptunnel = True

        forward_server, thread_process, error_no = ssh_tunnel_client.start_forward_server(local_port, remote_srvr,temptunnel=temptunnel)
        if error_no == SSH_NO_ERROR:
            remote_connect_info = ssh_tunnel_client.get_remote_connection_info()
            jump_host = None
            if remote_connect_info.jump != None:
                jump_host = remote_connect_info.jump.server.host_name
            forward_tunnel_info = SSHTunnelInfo(ssh_client=ssh_tunnel_client,
                                                service_group=service_group,
                                                service_name=service_name,
                                                jump_host=jump_host,
                                                remote_host=remote_host,
                                                remote_port=int(remote_port),
                                                local_port=int(local_port),
                                                dest_host=dest_host,
                                                dest_port=dest_port,
                                                cookie=str(service_data['cookie']),
                                                thread_process=thread_process,
                                                forward_server=forward_server,
                                                forward_tunnel=True)
            lock = threading.Lock()
            with lock:
                g_open_tunnel_list.append(forward_tunnel_info)
            if service_data['cookie'] != None:
                self.send_response("port:"+str(local_port)+",cookie:"+str(service_data['cookie'])+",msg:no_error")
            else:
                self.send_response("port:"+str(local_port)+",msg:no_error")
        else:
            self.send_response("port:-1,msg:cannot_start_forward_server")
        return

    def check_forward_tunnel_exist(self,jump_host,remote_host,dest_host,dest_port):
        global g_open_tunnel_list
        for tnnl in g_open_tunnel_list:
            if ((tnnl.forward_tunnel==True) and
                (jump_host == tnnl.jump_host) and
                (remote_host == tnnl.remote_host) and 
                (dest_host == tnnl.dest_host) and 
                (dest_port == tnnl.dest_port)):
                return tnnl.local_port
        return None
    
    def get_jump_remote_from_config(self,req_qry):
        if 'version' not in g_server_config_data:
            verbose("No config version, assuming v0.1")
            return self.get_jump_remote_from_config_v01(req_qry)
        if g_server_config_data['version'] == 'v0.1':
            verbose("Config version v0.1")
            return self.get_jump_remote_from_config_v01(req_qry)
        verbose("Unsupported configuration version: %s" % (g_server_config_data['version'],))
        return None

    def get_jump_remote_from_config_v01(self,req_qry):
        req_group_name   = req_qry['group'][0]
        req_service_name = req_qry['service'][0]
        for group_name in g_server_config_data:
            if group_name == req_group_name:
                service_group = g_server_config_data[group_name]
                for service_name in service_group:
                    if service_name == req_service_name:
                        service_data = service_group[service_name]
                        if (('host' not in service_data) or
                            ('protocol' not in service_data) or
                            ('authentication' not in service_data)):
                            return None
                        req_def = {}
                        req_def['service_group'] = req_group_name
                        req_def['service_name']  = service_name
                        req_def['username'] = None
                        if 'user' in service_data:
                            req_def['username'] = service_data['user']
                        req_def['remote_host'] = service_data['host']
                        req_def['jumphost'] = None
                        if 'jumphost' in service_data:
                            req_def['jumphost'] = service_data['jumphost']
                        req_def['jumpuser'] = None
                        if 'jumpuser' in service_data:
                            req_def['jumpuser'] = service_data['jumpuser']
                        req_def['tunneltype'] = "permanent"
                        if ('tunneltype' in service_data):
                            req_def['tunneltype'] = service_data['tunneltype']
                        req_def['conn'] = service_data['protocol']
                        req_def['auth'] = service_data['authentication']
                        return req_def
        return None

    def do_connect_port(self,req_qry):
        if "dhost" not in req_qry:
            self.send_response("port:-1,msg:missing_destination_host_in_request")
            return
        if "dport" not in req_qry:
            self.send_response("port:-1,msg:missing_destination_port_in_request")
            return        
        req_def = {}
        if (("group" in req_qry) or ("service" in req_qry)):
            if "service" not in req_qry:
                self.send_response("port:-1,msg:missing_service_in_request")
                return
            if "group" not in req_qry:
                self.send_response("port:-1,msg:missing_group_in_request")
                return
            req_def = self.get_jump_remote_from_config(req_qry)
            if req_def == None:
                self.send_response("port:-1,msg:no_matching_group_or_service_in_config_file")
                return 
        else:    
            if "rhost" not in req_qry:
                self.send_response("port:-1,msg:missing_remote_host_in_request")
                return
            req_def['remote_host'] = req_qry['rhost'][0]
            req_def['username'] = None
            if "ruser" in req_qry:
                req_def['username'] = req_qry['ruser'][0]
            req_def['jumphost'] = None
            req_def['jumpuser'] = None
            if "jhost" in req_qry:
                req_def['jumphost'] = req_qry['jhost'][0]
                if "juser" in req_qry:
                    req_def['jumpuser'] = req_qry['juser'][0]
            req_def['auth'] = "Password/Passcode"
        req_def['dest_host']  = req_qry['dhost'][0]
        req_def['dest_port']  = req_qry['dport'][0]

        connected_dest_port = self.check_forward_tunnel_exist(req_def['jumphost'],req_def['remote_host'],
                                                              req_def['dest_host'],int(req_def['dest_port']))
        if connected_dest_port != None:
            self.send_response("port:"+str(connected_dest_port)+",msg:no_error")
            return
            
        ssh_connect_remote = self.login_connect_remote(req_def)
        if ssh_connect_remote == None:
            self.send_response("port:-1,msg:cannot_connect_to_remote_host_port")
            return
        
        ssh_tunnel_client = SSHLocalRemoteTunnel(ssh_connect_remote)

        remote_host = req_def['remote_host']
        remote_port = SSH_PORT
        dest_host   = req_def['dest_host']
        dest_port   = int(req_def['dest_port'])
        remote_srvr = SSHServerInfo(dest_host,dest_port)
        
        local_port = self.get_local_port(requested_port=None)
        if local_port == None:
            self.send_response("port:-1,msg:cannot_find_local_port")
            return
        
        temptunnel = False
        if "transient" in req_def['tunneltype']:
            temptunnel = True
 
        forward_server, thread_process, error_no = ssh_tunnel_client.start_forward_server(local_port, remote_srvr,temptunnel=temptunnel)
        if error_no == SSH_NO_ERROR:
            remote_connect_info = ssh_tunnel_client.get_remote_connection_info()
            jump_host = None
            if remote_connect_info.jump != None:
                jump_host = remote_connect_info.jump.server.host_name
            forward_tunnel_info = SSHTunnelInfo(ssh_client=ssh_tunnel_client,
                                                jump_host=jump_host,
                                                remote_host=remote_host,
                                                remote_port=int(remote_port),
                                                local_port=int(local_port),
                                                dest_host=dest_host,
                                                dest_port=dest_port,
                                                thread_process=thread_process,
                                                forward_server=forward_server,
                                                forward_tunnel=True)
            lock = threading.Lock()
            with lock:
                g_open_tunnel_list.append(forward_tunnel_info)
            self.send_response("port:"+str(local_port)+",msg:no_error")
        else:
            self.send_response("port:-1,msg:cannot_start_forward_server")
        return

    def do_reverse_port_forward(self,req_qry):
        print("Reverse port forwarding request: ", req_qry)
        if 'group' not in req_qry:
            self.send_response("port:-1,msg:missing_group_in_request")
            return
        if 'service' not in req_qry:
            self.send_response("port:-1,msg:missing_service_in_request")
            return
        if 'dest_host' not in req_qry:
            self.send_response("port:-1,msg:missing_dest_host_in_request")
            return
        if 'remote_port' not in req_qry:
            self.send_response("port:-1,msg:missing_remote_port_in_request")
            return
        if 'dest_port' not in req_qry:
            self.send_response("port:-1,msg:missing_dest_port_in_request")
            return
        
        req_def = self.parse_service_request(req_qry)
        if req_def == None:
            self.send_response("port:-1,msg:incomplete_service_definition")
            return
        req_def['dest_host'] = req_qry['dest_host'][0]
        req_def['dest_port'] = req_qry['dest_port'][0]
        req_def['remote_port'] = req_qry['remote_port'][0]
        
        print("Reverse port request: ",req_def)
        self.execute_reverse_port_forwarding(req_def)
        return

    def execute_reverse_port_forwarding(self,req_def):
        global g_open_tunnel_list
        print("Request :", req_def)

        ssh_connect_remote = self.login_connect_remote(req_def)
        if ssh_connect_remote == None:
            self.send_response("port:-1,msg:cannot_connect_to_remote_host_port")
            return

        remote_port = int(req_def['remote_port'])
        dest_host   = req_def['dest_host']
        dest_port   = int(req_def['dest_port'])
        ssh_tunnel_client = SSHLocalRemoteTunnel(ssh_connect_remote)

        thread_process = ssh_tunnel_client.reverse_forward_tunnel(remote_server_port=remote_port,
                                                                  dest_host=dest_host,
                                                                  dest_port=dest_port)
        if thread_process == None:
            self.send_response("port:-1,msg:cannot_connect_to_remote_host_port")
            return
        
        reverse_tunnel_info = SSHTunnelInfo(ssh_client=ssh_tunnel_client,
                                            remote_host=req_def['remote_host'],
                                            remote_port=remote_port,
                                            dest_host=dest_host,
                                            dest_port=dest_port,
                                            thread_process=thread_process,
                                            reverse_tunnel=True)
        lock = threading.Lock()
        with lock:
            g_open_tunnel_list.append(reverse_tunnel_info)
        self.send_response("port:"+str(reverse_tunnel_info['remote_port'])+"msg:no_error")

        return

def parse_arguments(argv):
    server_port = PORT
    config_file = REMOTE_CONFIG_FILE
    try:
        opts, args = getopt.getopt(argv, "hc:p:")
    except getopt.GetoptError:
        print('python ssh_tunnel_server.py -c <config file> -p <server port>')
        sys.exit(2)
    for opt, arg in opts:
        if opt =='-h':
            print('python ssh_tunnel_server.py -c <config file> -p <server port>')
            sys.exit(0)
        elif opt =='-c':
            config_file = arg
        elif opt =='-p':
            server_port = int(arg)
    return config_file, server_port

def removeTunnel(dest_host,dest_port):
    for tnnl in g_open_tunnel_list:
        if ((tnnl.forward_tunnel==True) and
            (dest_host == tnnl.dest_host) and 
            (dest_port == tnnl.dest_port)):
            print("Shutting down: ",dest_host," port: ",dest_port)
            tnnl.forward_server.shutdown()
            tnnl.forward_server.server_close()
            g_open_tunnel_list.remove(tnnl)
            
def run_queue():
    global g_open_tunnel_list
    while (True):
        (dest_host,dest_port) = g_queue.get()
        print("QUEUE :",dest_host," port: ",dest_port)
        lock = threading.Lock()
        with lock:
            removeTunnel(dest_host,dest_port)

class ReuseAddrTCPServer(SocketServer.TCPServer):
    allow_reuse_address = True

def start_server(argv):
    global g_remote_conn_list
    global g_open_tunnel_list
    global g_server_config_data
    g_remote_conn_list = []
    g_open_tunnel_list = []
    config_file, server_port = parse_arguments(argv)
    if os.path.isfile(config_file) ==True:
        g_server_config_data = read_yaml_config(config_file)
    else:
        g_server_config_data = {}

    thread = threading.Thread(target=run_queue,args=())
    thread.daemon = True
    thread.start()
    while thread.is_alive() == False:
        time.sleep(THREAD_CHECK_TIME)

#    SocketServer.TCPServer.allow_reuse_address = True
#    with SocketServer.TCPServer((HOST, server_port),MyTCPHandler) as server:
    with ReuseAddrTCPServer((HOST, server_port),MyTCPHandler) as server:
        print("SSH Tunnel Server: ", HOST,server_port)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("Closing")
            server.server_close()
        except Exception as e:
            print("Exception: ", e)
            server.server_close()


if __name__ == "__main__":
    start_server(sys.argv[1:])
