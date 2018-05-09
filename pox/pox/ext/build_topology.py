import os
import sys
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.node import OVSController
from mininet.node import Controller
from mininet.node import RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.util import pmonitor
from mininet.util import quietRun, natural, custom
sys.path.append("../../")
from pox.lib.util import dpid_to_str
from pox.ext.jelly_pox import JELLYPOX
from subprocess import Popen
from time import sleep, time

from jelly_graph import build_graph, add_servers, get_paths
import pickle
import random

import re
from sys import exit, stdout, stderr


#n_switch = 200
#n_interswitch_ports = 12
#n_server_oports = 24
#n_servers = 686

n_switch = 8
n_interswitch_ports = 4
n_server_oports = 5
n_servers = 20

all_servers = []
all_switches = []

def info(s):
    print s

def dictFromList( items ):
    "Return dict[1..N] from list of items"
    return dict( zip( range( 1, len( items ) + 1 ), items ) )

def listening( src, dest, port=5001 ):
    return True
    "Return True if we can connect from src to dest on port"
    cmd = 'echo A | telnet -e A %s %s' % (dest.IP(), port)
    result = src.cmd( cmd )
    return 'Connected' in result

def pct( x ):
    "pretty percent"
    return round(  x * 100.0, 2 )
def parseIntfStats( startTime, stats ):
    """Parse stats; return dict[intf] of (s, rxbytes, txbytes)
       and list of ( start, stop, user%... )"""
    spaces = re.compile('\s+')
    colons = re.compile( r'\:' )
    seconds = re.compile( r'(\d+\.\d+) seconds')
    intfEntries, cpuEntries, lastEntries = {}, [], []
    for line in stats.split( '\n' ):
        m = seconds.search(line)
        if m:
            s = round( float( m.group( 1 ) ) - startTime, 3 )
        elif '-eth' in line:
            line = spaces.sub( ' ', line ).split()
            intf = colons.sub( '', line[ 0 ] )
            rxbytes, txbytes = int( line[ 1 ] ), int( line[ 9 ] )
            intfEntries[ intf ] = intfEntries.get( intf, [] ) +  [
                    (s, rxbytes, txbytes ) ]
        elif 'cpu ' in line:
            line = spaces.sub( ' ', line ).split()
            entries = map( float, line[ 1 : ] )
            if lastEntries:
                dtotal = sum( entries ) - sum( lastEntries )
                if dtotal == 0:
                    raise Exception( "CPU was stalled from %s to %s - giving up" %
                                     ( lastTime, s ) )
                deltaPct = [ pct( ( x1 - x0 ) / dtotal ) 
                             for x1, x0 in zip( entries, lastEntries) ]
                interval = s - lastTime
                cpuEntries += [ [ lastTime, s ] + deltaPct ]
            lastTime = s
            lastEntries = entries

    return intfEntries, cpuEntries

def remoteIntf( intf ):
    "Return other side of link that intf is connected to"
    link = intf.link
    return link.intf1 if intf == link.intf2 else link.intf2

# from mininet-tests/pairs/pair_intervals.py
def iperfPairs(clients, servers):
    optstime = 60
    "Run iperf semi-simultaneously one way for all pairs"
    pairs = len( clients )
    plist = zip( clients, servers )
    info( '*** Clients: %s\n' %  ' '.join( [ c.name for c in clients ] ) )
    info( '*** Servers: %s\n' %  ' '.join( [ c.name for c in servers ] ) )
    info( "*** Shutting down old iperfs\n")
    quietRun( "pkill -9 iperf" )
    info( "*** Starting iperf servers\n" )
    for dest in servers:
        dest.cmd( "iperf -s &" )
    info( "*** Waiting for servers to start listening\n" )
    for src, dest in plist:
        info( dest.name )
        sleep( .5 )
        while not listening( src, dest ):
            info( '.' )
            sleep( .5 )
    info( '\n' )
    info( "*** Starting iperf clients\n" )
    for src, dest in plist:
        src.sendCmd( "sleep 1; iperf -t %s -i .5 -P 8 -c %s" % (
            optstime, dest.IP() ) )
    info( '*** Running cpu and packet count monitor\n' )
    startTime = int( time() )
    cmd = "./packetcount %s .5" % ( optstime + 2 )
    stats = quietRun( cmd  )
    intfEntries, cpuEntries = parseIntfStats( startTime, stats )
    info( "*** Waiting for clients to complete\n" )
    results = []
    for src, dest in plist:
        info( "*\n" )
        result = src.waitOutput()
        dest.waiting = False
        dest.cmd( "kill -9 %iperf" )
        # Wait for iperf server to terminate
        dest.cmd( "wait" )
        # We look at the stats for the remote side of the destination's
        # default intf, as it is 1) now in the root namespace and easy to
        # read and 2) guaranteed by the veth implementation to have
        # the same byte stats as the local side (with rx and tx reversed,
        # naturally.)  Otherwise
        # we would have to spawn a packetcount process on each server
        intfName = remoteIntf( dest.defaultIntf() ).name
        intervals = intfEntries[ intfName ]
        # Note: we are reversing txbytes and rxbytes to reflect
        # the statistics *at the destination*
        results += [ { 'src': src.name, 'dest': dest.name,
                    'destStats(s,txbytes,rxbytes)': intervals } ]
    info( "done" )
    return results, cpuEntries

def pairTest(clients, servers):
    """Run a set of tests for a series of counts, returning
        accumulated iperf bandwidth per interval for each test."""
    results = []
    #initOutput( opts.outfile )
    # 9 categories in linux 2.6+
    cpuHeader = ( 'cpu(start,stop,user%,nice%,sys%,idle%,iowait%,'
                 'irq%,sirq%,steal%,guest%)' )
    for pairs in [1]:
        #net.start()
        intervals, cpuEntries = iperfPairs(clients, servers )
        #net.stop()
        # Write output incrementally in case of failure
        result = { 'pairs': pairs, 'results': intervals,
            cpuHeader: cpuEntries }
        #appendOutput( opts, [ result ] )
        results += [ result ]
    return results

def int_to_mac(i):
    s = ("%0.2X" % i).zfill(12)
    return s[0:2] + ":" + s[2:4] + ":" + s[4:6] + ":" + s[6:8] + ":" + s[8:10] + ":" + s[10:12]

class JellyFishTop(Topo):
    ''' TODO, build your topology here'''
    def build(self):
        self.z = 99
        self.perm = range(n_servers)
        random.shuffle(self.perm)
        S = 512
        graph = build_graph(n_switch, n_interswitch_ports, n_server_oports, n_servers)
        serv_switch, switch_serv = add_servers(n_switch, n_server_oports, n_servers)
        paths = get_paths(self.perm, graph, serv_switch, 8, n_servers, "kshortest")
        print graph, serv_switch, paths

        bw = 5
        
        pickle.dump(paths, open("paths.pkl", "w"))

        self.px_switches = []
        for i in range(n_switch):
            switch = self.addSwitch('s'+str(i+1), dpid=format(i+1, '012'))
            self.px_switches.append(switch)

        links = set()
        for i in range(n_switch):
            for j in graph[i]:
                # to ensure that we don't add a link twice (since they are bidirectional)
                if (i,j) not in links and (j,i) not in links:
                    self.addLink(self.px_switches[i], self.px_switches[j], port1=j+1, port2=i+1, bw=bw)
                    links.add((i, j))

        self.px_servers = []
        for i in range(n_servers):
            server = self.addHost('h'+str(i), mac=int_to_mac(S+i+1))
            self.px_servers.append(server)
            myswitch = serv_switch[i]
            px_myswitch = self.px_switches[myswitch]
            self.addLink(server, px_myswitch, port=1, port2=S+i+1, bw=bw)

        
        '''
        paths = {
            (S+0,S+1) : [[0, 1], [0,2,3,1]],
            (S+0,S+2) : [[0, 2], [0,1,3,2]],
            (S+0,S+3) : [[0, 2], [0,1,3,2]],
        }
        '''

        '''
        graph = {
            0: {1,2},
            1: {0,3},
            2: {0,3},
            3: {1,2}
        }
        serv_switch = { 0:0, 1:1, 2:2, 3:3 }
        paths = get_paths(graph, serv_switch, 2, n_servers)
        pickle.dump(paths, open("paths.pkl", "w"))

        S = 500
        print "HEY", int_to_mac(S+1)
        h1 = self.addHost( 'h0', mac=int_to_mac(S+1))
        h2 = self.addHost( 'h1', mac=int_to_mac(S+2))
        h3 = self.addHost( 'h2', mac=int_to_mac(S+3))
        h4 = self.addHost( 'h3', mac=int_to_mac(S+4))
        s1 = self.addSwitch( 's0', dpid=format(1, '012'))
        s2 = self.addSwitch( 's1', dpid=format(2, '012'))
        s3 = self.addSwitch( 's2', dpid=format(3, '012'))
        s4 = self.addSwitch( 's3', dpid=format(4, '012'))

        self.addLink(h1,s1, port1=1, port2=S+1)
        self.addLink(h2,s2, port1=1, port2=S+2)
        self.addLink(h3,s3, port1=1, port2=S+3)
        self.addLink(h4,s4, port1=1, port2=S+4)

        # Add links
        self.addLink(s1,s2, port1=2, port2=1)
        self.addLink(s2,s4, port1=4, port2=2)
        self.addLink(s3,s4, port1=4, port2=3)
        self.addLink(s1,s3, port1=3, port2=1)
        '''

def experiment(net):
    net.start()
    sleep(3)
    
    print net
    print net.topo
    print net.topo.z
    all_servers = map(lambda x: net.get(x), net.topo.px_servers)
    all_switches = map(lambda x: net.get(x), net.topo.px_switches)
    print all_servers, all_switches
    print("hey", type(all_servers[0]))
    #net.pingAll()
    mysenders = []
    myreceivers = []
    server_perm = net.topo.perm
    print len(all_servers)
    print len(all_switches)
    for i in range(n_servers-1):
        mysenders.append(all_servers[server_perm[i]])
        myreceivers.append(all_servers[server_perm[i+1]])
    mysenders.append(all_servers[server_perm[-1]])
    myreceivers.append(all_servers[server_perm[0]])

    print server_perm
    print mysenders, mysenders

    #results = pairTest(mysenders, myreceivers)
    net.pingAll()
    print results
    

    '''
    popens = {}
    for sender, receiver in zip(mysenders, myreceivers):
        print "adding ", sender, receiver
        print "iperf -c "+receiver.IP()+" -P 1"
        #receiver.sendCmd("iperf -s -p 1")
        #cliout = sender.cmd('iperf -t 10 -c '+receiver.IP()+' -p 1')
        #print cliout
        r = net.iperf([sender, receiver], l4Type="TCP", port=1)
        print r
        #popens[ sender ] = sender.popen("iperf -t 30 -c "+receiver.IP()+" -i 1 -p 1")
        #receiver.popen("iperf -s -p 1")
        #popens[ sender ] = sender.popen("iperf -t 30 -c "+receiver.IP()+" -i 1 -p 1")
        #popens[ sender ] = sender.popen("ping -c5 %s" % receiver.IP())
    print "started all iperfs"
    '''
    # Monitor them and print output
    '''
    for host, line in pmonitor( popens ):
        if host:
            print( "<%s>: %s" % ( host.name, line ) )
    '''
    # Done
    net.stop()

def main():
    topo = JellyFishTop()
    #setLogLevel('debug')
    net = Mininet(topo=topo, host=CPULimitedHost, link = TCLink, controller=JELLYPOX, autoStaticArp=True)
    experiment(net)

if __name__ == "__main__":
    main()

