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

class RQ(SimpleXMLRPCRequestHandler):
   def __init__(self, request, client_address, server):
      print client_address
      SimpleXMLRPCRequestHandler.__init__(self, request, client_address, server)

class PeerHandler(StreamRequestHandler):
   def setup(self):
      print "PeerHandler setup"
      pass

   def handle(self):
      print "PeerHandler handle"
      pass

   def finish(self):
      print "PeerHandler finish"
      pass

class Master:
   def __init__(self, configfile):
      config = yaml.load(open(configfile, "r"))
      name = socket.getfqdn()
      self.params = {}
      self.publishers = { name: {} }
      self.subscribers = { name: {} }
      self.name = name
      self.static_peers = config['peers']
      self.peers = {}
      self.peer_connections = {}
      self.done = False

      print "Peers: "
      for p in self.static_peers:
         print p

      self.peer_server = ThreadingTCPServer(("", int(config['port'])), 
         PeerHandler)
      self.peer_server_t = threading.Thread(target = self.peer_server.serve_forever)
      self.peer_server_t.start()

      self.ping_thread = threading.Thread(target = self.ping_peers)
      self.ping_thread.start()


   # thread that periodically pings and cleans up peers
   def ping_peers(self):
      while not self.done:
         for p in self.peers:
            # send a ping
            print "Pinging %s"%p

         for p in self.static_peers:
            if not p in self.peers:
               # try to establish contact with peer
               print "Trying to contact %s"%p
         
         # sleep
         time.sleep(1)

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
      print "registerSubscriber"
      publishers = []
      if topic in self.publishers[self.name]:
         for p in self.publishers[self.name][topic]:
            publishers.append("http://localhost:%d/"%p)
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
      if not topic in self.publishers[self.name]:
         self.publishers[self.name][topic] = set()
      self.publishers[self.name][topic].add(int(port))
      # TODO: notify subscribers
      return 1, "Registered [%s] as publisher of [%s]"%(caller_id, topic), []

   def unregisterPublisher(self, caller_id, topic, caller_api):
      print "unregisterPublisher"
      host,port = parse_uri(caller_api)
      if topic in self.publishers[self.name]:
         self.publishers[self.name][topic].discard(int(port))
      return 1


def main():
   configfile = sys.argv[1]
   port = int(parse_uri(os.getenv("ROS_MASTER_URI"))[1])
   print "Binding to port %d"%port
   server = SimpleXMLRPCServer(("", port), RQ) # bind to port 11311, all addresses
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
