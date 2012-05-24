#!/usr/bin/env python
#
# Peer-to-peer ROS Master
# Proof-of-concept
#
# Theory: 
#  All nodes talk to a ROS master that lives on their local machine; this
#  master then discovers other masters on the network and builds a shared table
#  of topics, services and parameters with them. 
#
#  The master is responsible for coordinating network transport as well; it 
#  understands the network topology (via connections to its peers), subscribes
#  to machine-local topics and publishes them for remote consumption as
#  appropriate
#
# 
# For this proof-of-concept master, the following features are implemented:
# * Parameter set and get
# * getPid
# * register and unregister publishers
# * register subscribers
# * coordinate with a static list of peers
# * exchange publisher lists with static peers
#
# Running:
#  set ROS_MASTER_URI as appropriate
#  provide a configuration file that lists the master URIs of peers to contact
#
# Usage:
#  ROS_MASTER_URI=http://<name>:<port>
#  p2p-master.py <config.yaml>

import os
import sys
import socket
import yaml
import threading
import time
import select

from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler
#from SocketServer import ThreadingUDPServer,DatagramRequestHandler
from SocketServer import ThreadingTCPServer,StreamRequestHandler

def parse_uri(uri):
   # strip http://
   base = uri.lstrip('http://')
   # strip trailing path /...
   hostport = base.partition('/')[0]
   # split host and port number
   (host,sep, port) = hostport.partition(':')
   return host, port

class Master:
   def __init__(self, configfile):
      config = yaml.load(open(configfile, "r"))
      name = socket.getfqdn()
      name = name.replace(".", "_") # replace periods with underscores
      self.params = {}
      self.publishers = { name: {} }
      self.subscribers = { name: {} }
      self.name = name
      self.static_peers = config['peers']
      self.peers = {}
      self.peer_connections = {}
      self.peer_names = { name: name }
      self.done = False

      print "Peers: "
      for p in self.static_peers:
         print p

      try:
         self.peer_socket = socket.socket()
         self.peer_socket.bind(('', config['port']))
         self.peer_socket.listen(10)
      except:
         print "Failed to bind to multimaster socket"
         sys.exit(-1)

      self.peer_thread = threading.Thread(target = self.peer_talk)
      self.peer_thread.start()


   # thread that periodically pings and cleans up peers
   def peer_talk(self):
      while not self.done:
         # sleep
         time.sleep(1)

         # accept incoming connections
         r,w,e = select.select([self.peer_socket], [], [], 1.0)
         while self.peer_socket in r:
            client = self.peer_socket.accept()
            print client
            remote = "%s:%d"%client[1]
            remote = client[1][0]
            sock = client[0]
            sock.setblocking(0)
            sock.send(yaml.dump([self.name, self.publishers[self.name]]))
            print remote
            if remote not in self.peers:
               self.peers[remote] = []
            self.peers[remote].append([sock, time.time()])
            r,w,e = select.select([self.peer_socket], [], [], 1.0)

         # ping peers and read incoming data
         dead_peers = []
         for p in self.peers:
            # send a ping
            print "Pinging %s"%p
            for i,s in enumerate(self.peers[p]):
               # Send Ping
               try:
                  s[0].send(yaml.dump([self.name, self.publishers[self.name]]))
               except:
                  # if we had trouble sending a ping, close the socket
                  try:
                     s[0].close()
                  except:
                     pass
                  del self.peers[p][i]

               # Receive data (pings + other)
               try:
                  r = s[0].recv(4096)
               except socket.error:
                  pass
               else:
                  if len(r) > 0:
                     print "Got data from %s"%p
                     print r
                     if len(r) > 1:
                        data = yaml.load(r)
                        self.peer_names[p] = data[0]
                        self.publishers[p] = data[1]
                     s[1] = time.time()

               # check timeout
               if s[1] + 15 < time.time():
                  print s, " timed out"
                  s[0].close()
                  del self.peers[p][i]

            # if we have no more connections to this peer, consider it dead
            if len(self.peers[p]) < 1:
               print "Connection to %s lost."%p
               dead_peers.append(p)

         # delete peers that were dead
         for d in dead_peers:
            del self.peers[d]

         # try to contact our static peers
         for p in self.static_peers:
            # try to establish contact with peer
            host,sep,port = p.partition(':')
            try:
               host = socket.gethostbyname(host)
            except:
               pass # ignore failures in name resolution
            if host not in self.peers:
               try:
                  sock = socket.create_connection((host,port))
                  if sock:
                     sock.setblocking(0)
                     sock.send(yaml.dump([self.name, self.publishers[self.name]]))
                     if host not in self.peers:
                        self.peers[host] = []
                     self.peers[host].append([sock, time.time()])
                  else:
                     print "Failed to contact %s"%p
               except:
                  print "Failed to contact %s"%p

   # stub
   def shutdown(self, caller_id, msg=''):
      self.done = True
      return 1, "shutdown", 0

   # stub
   def getUri(self, caller_id):
      return 1, "", ""

   def getPid(self, caller_id):
      return 1, "", os.getpid()

   # stub
   def deleteParam(self, caller_id, key):
      return 1, "", 0

   def setParam(self, caller_id, key, value):
      path = key.split('/')
      param = self.params
      for p in path[1:-1]:
         if not p in param:
            param[p] = {}
         param = param[p]
      param[path[-1]] = value
      return 1, "", 0

   def getParam(self, caller_id, key):
      path = key.split('/')
      param = self.params
      for p in path[1:]:
         param = param[p]
      return 1, "", param
   
   # stub
   def searchParam(self, caller_id, key):
      return 1, "", ""

   # stub
   def subscribeParam(self, caller_id, caller_api, key):
      return 2, "", ""

   # stub
   def unsubscribeParam(self, caller_id, caller_api, key):
      return 1, "", 0

   # stub
   def hasParam(self, caller_id, key):
      return 1, key, False

   # stub
   def getParamNames(self, caller_id):
      return 1, "Parameter names", []

   def registerSubscriber(self, caller_id, topic, topic_type, caller_api):
      print "registerSubscriber: %s"%topic
      publishers = []
# This _ought_ to allow local clients to subscribe to remote systems by 
#  specifying a remote prefix and a topic name, but it fails because the
#  subscriber and publisher don't agree on the topic name
# This can probably be fixed by proxying topics through the masters, but
#  that isn't required for the proof-of-concept
#      path = topic.split('/')[1:]
#
#      if len(path) > 1:
#         prefix = path[0]
#         remote_topic = "/%s"%('/'.join(path[1:]))
#         print "looking in remote prefix %s"%prefix
#         for peer in self.publishers:
#            if self.peer_names[peer] == prefix:
#               print "Found peer %s"%peer
#               print "Looking for remote topic %s"%remote_topic
#               if remote_topic in self.publishers[peer]:
#                  print "Found remote topic %s"%remote_topic
#                  for p in self.publishers[peer][remote_topic]:
#                     host = peer
#                     if peer == self.name:
#                        host = "localhost"
#                     publishers.append("http://%s:%d/"%(host, p))
#      else:
#         print "local subscribe"
#      if len(publishers) == 0:
#         print "looking in local topics"
#         if topic in self.publishers[self.name]:
#            for p in self.publishers[self.name][topic]:
#               publishers.append("http://%s:%d/"%("localhost", p))
      # look for topics anywhere with the given name
      for peer in self.publishers:
         if topic in self.publishers[peer]:
            for p in self.publishers[peer][topic]:
               host = peer
               if peer == self.name:
                  host = "localhost"
               publishers.append("http://%s:%d/"%(host, p))
      print publishers
      # TODO: add to list of subscribers
      return 1, "Subscribed to [%s]"%topic, publishers
   
   # TODO: stub
   def unregisterSubscriber(self, caller_id, topic, caller_api):
      print "unregisterSubscriber"
      return 1

   def registerPublisher(self, caller_id, topic, topic_type, caller_api):
      print "registerPublisher"
      host,port =  parse_uri(caller_api)
      if topic not in self.publishers[self.name]:
         self.publishers[self.name][topic] = set()
      self.publishers[self.name][topic].add(int(port))
      # TODO: notify subscribers
      # TODO: notify peers
      return 1, "Registered [%s] as publisher of [%s]"%(caller_id, topic), []

   def unregisterPublisher(self, caller_id, topic, caller_api):
      print "unregisterPublisher"
      host,port = parse_uri(caller_api)
      if topic in self.publishers[self.name]:
         self.publishers[self.name][topic].discard(int(port))
      return 1

   def getSystemState(self, caller_id):
      pub = []
      for peer in self.publishers:
         for topic in self.publishers[peer]:
            ports = []
            for p in self.publishers[peer][topic]:
               ports.append(p)
            if peer == self.name:
               pub.append([topic, ports])
            pub.append(["/%s%s"%(self.peer_names[peer], topic), ports])
      sub = []
      ser = []
      return 1, "current system state", [pub, sub, ser]


def main():
   configfile = sys.argv[1]
   port = int(parse_uri(os.getenv("ROS_MASTER_URI"))[1])
   print "Binding to port %d"%port
   server = SimpleXMLRPCServer(("", port)) # bind to port 11311, all addresses
   master = Master(configfile)
   server.register_multicall_functions()
   server.register_instance(master)
   print "Ready?"
   try:
      server.serve_forever()
   except:
      # TODO: kill peer server
      master.shutdown('local')
      print "Done"

if __name__ == '__main__':
   main()
