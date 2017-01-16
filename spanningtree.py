# encoding: utf-8

"""
This is a test of spanning trees - eventually to find its way into web42 or the Arduinio

Intention is that this creates a spanning tree of http connected nodes, and can ask up the tree to get to any node,
can also get the closest node to a notional node in a disconnected network, (including where the node is not connected)

#TODO - retries - if message fails, try another route <<< TESTING THIS IN SIM3
# Figure out why it actually drops likelihood fo connecting at higher connection densities - can test even at 10 nodes
# -- Make the analysis preditive e.g. test 100 connections (30*30 is too many)
#TODO - make sure have at least two paths to a node
#TODO - maybe know about third or further degree connections
#TODO - send info when connect as well as retrieve (receiver then uses for own optimisation, attempt at reverse connection etc)
#TODO - get median connection distance,
#TODO - report on sim - no conenctions, median #connections,
#TODO - try keeping a few random long-distance connections
#TODO - make this threaded - each Node is a thread
"""

from random import randint
import numpy

debug = True

class Node(object):
    """
    Represents a node somewhere in the net. This allows for simulations across multiple nodes.
    Note that a "Peer" is the representation inside a node of another node.
    """
    optimaxconnections = 10
    optiminconnections = 1

    def __init__(self, nodeid=None):
        self.peers = PeerSet()     # Will hold list of peers we know about
        self.nodeid = nodeid if nodeid else randint(1,2**10-1)

    def __repr__(self):
        return "Node(%d)" % self.nodeid

    def __eq__(self, other):
        if not isinstance(other, int):
            other = other.nodeid
        return self.nodeid == other

    def setminmax(self, min, max):
        # Set min and max connections, (None means don't set)
        if min: optiminconnections = min
        if max: optimaxconnections = max

    def onconnected(self, peer):
        """
        Should be called when connect to peer.
        """
        #if debug: print "Onconnecting",self,"<",peer
        peer.connected = True
        pdiclist = peer.reqpeers()  # So we know who our peers are conencted to
        peer.setcachedpeers(self,pdiclist)

        for peerspeer in peer.cachedpeers:
            if self.closer(peer, peerspeer) and peerspeer.connected:
                #print "connected=", len(self.peers.connected()), "optimax=",self.optimaxconnections
                if len(self.peers.connected()) > self.optimaxconnections:
                    # Peer is connected to Peerspeer and is closer to us, so drop peerspeer and just connect to peer
                    peerspeer.disconnect(connecttheirpeers=False,
                                         reason="Connected via closer new peer %d and have %d/%d cconnected" %
                                                (peer.nodeid, len(self.peers.connected()), self.optimaxconnections ))

    def ondisconnected(self, peer):
        """
        Should be called when disconnected from a peer
        Should decide which of their peers we want to connect to directly.
        """
        for peerspeer in peer.cachedpeers:
            peerspeer.connectedvia.remove(peer)            # XXXTODO not valid
        self.considerconnecting()

    def considerconnecting(self):
        """
        Decide if want to connect to any peers that we know about that aren't connected
        XXX Think this through - want to aggressively add connections below min, prune above max,
        levels from one extreme to other ...
        prune anything can get to another way
        prune anything can get to via 1 hop
        add anything have no way to reach
        add anything cant get to via 1 hop
        add anything 1 hop away
        """
        if len(self.peers.connected()) > self.optimaxconnections:
            candidate = self.peers.connectedandviacloserpeer().furthestfrom(self) # We can get to it via something else
            if candidate:
                candidate.disconnect(reason="Dist=%d and connected via %s and have too many connections %d/%d" %
                                            (self.distance(candidate),["%d@%d" % (p.nodeid,self.distance(p)) for p in candidate.connectedvia], len(self.peers.connected()), self.optimaxconnections))


        elif len(self.peers.connected()) < self.optiminconnections:
            candidate = self.peers.notconnectedandnotviapeer().closestto(self) or self.peers.notconnected().closestto(self) # Look for closest cant reach and if none, just closest
            if candidate:
                candidate.connect()
            # TODO should really be a bit more random else one close unconnectable peer will block everything
        else:   # Between min and max, try connecting one that can't get via peer
            candidate = self.peers.notconnectedandnotviapeer().closestto(self)
            if candidate:
                candidate.connect()
            # TODO should really be a bit more random else one close unconnectable peer will block everything

    def closer(self,peer, peerspeer): # True if peer is closer than peerspeer
        return self.distance(peer) < self.distance(peerspeer)

    def distance(self, peer):
        offset = peer.nodeid ^ self.nodeid
        return bin(offset).count("1")

    def handlereqpeers(self):
        """
        Called when contacted (typically over network) for list of peers
        """
        return [ peer.dict() for peer in self.peers.connected() ]

    def seedpeer(self, nodeid=None, ipaddr=None):
        if isinstance(nodeid, Node): nodeid=nodeid.nodeid
        if nodeid == self.nodeid:
            return None
        peer = self.peers.find(nodeid)
        if not peer:
            peer = Peer(node=self, nodeid=nodeid, ipaddr=ipaddr)
        self.peers.append(peer)
        return peer

    def debugprint(self, level=2):
        if level == 2: # Print node, and each of its peers on its own line
            print "Node(%d)" % self.nodeid
            for peer in self.peers:
                print "     "+peer.debugline(level=level)
        elif level == 1:
            print self.nodeid,":",[peer.nodeid for peer in self.peers.connected()]

    def sendMessage(self, msg):
        """
        Send a message to nodeid
        If its for us, deliver and return 0
        If its directly connected, pass to peer
        If we know how to reach it, send via that node
        If we don't know then send through the closest node we are connected to.
        Returns the number of steps to the node (this node is 1, 0 is not reachable
        """
        verbose = msg.verbose
        hops = msg.hops # Keep a local copy of msg hops at call, don't increment as keep trying to send
        if msg.nodeid == self:
            if verbose: print "Message received at nodeid",msg.nodeid
            msg.hops += 1
            return PeerResponse(success=True, msg=msg)
        else:
            peer = self.peers.find(msg.nodeid)
            if peer:
                if peer.connected:
                    # Can't have been "tried' since its the destination
                    if verbose: print self,"Sending to peer",msg.nodeid
                    msg.hops = hops+1
                    return peer.sendMessage(msg) # Response should reflect updated hops
                else: # Not connected, but we know of it
                    # target is not connected but we know of it, so try anything that it is connectedvia that we are connected to and haven't already tried.
                    for intermediate in peer.connectedvia.connected().notin(msg.tried):
                        msg.tried.append(intermediate)
                        if verbose: print "Sending to intermediate",intermediate,"for",msg.nodeid
                        msg.hops = hops+1
                        res = intermediate.sendMessage(msg)
                        msg.tried.append(res.tried) # Accumulate any places it tried (should already include everything tried previously)
                        if res.success: return res       # Return if successful else will try others
                    # If none of them work, drop through and try for closest
            # Try all connected nodes, in order of how close to target
            intermediate = self.peers.connected().closestto(msg.nodeid, exclude=msg.tried)
            while intermediate:
                if verbose: print self,"Sending via closest", intermediate, "for", msg.nodeid
                msg.tried.append(intermediate)
                msg.hops = hops+1
                res = intermediate.sendMessage(msg)
                if res.success: return res
                msg.tried.append(res.tried)
                if verbose: print self, "Retrying from ",self.nodeid,"to destn",msg.nodeid,"with excluded",msg.tried
                intermediate = self.peers.connected().closestto(msg.nodeid, exclude=msg.tried)  # Try next closest untried
            # Tried all of them - fail
            if verbose: print self,"No next step towards",msg.nodeid
            return PeerResponse(success=False, msg=msg, err="No route to host")

    def loop(self):
        """
        Frequently acted on
        # TODO hook this to the sim
        """
        self.considerconnecting()


class PeerSet(set):
    """
    A list of peers
    """

    def connected(self):
        return PeerSet([ peer for peer in self if peer.connected ])

    def notconnectedandnotviapeer(self):
        return PeerSet([ peer for peer in self if not peer.connected and not peer.connectedvia ])

    def connectedandviapeer(self):
        return PeerSet([ peer for peer in self if peer.connected and peer.connectedvia ])

    def connectedandviacloserpeer(self):
        return PeerSet([ peer for peer in self if peer.connected and peer.connectedvia and any([p.closer(peer) for p in peer.connectedvia ]) ])

    def notconnected(self):
        return PeerSet([ peer for peer in self if not peer.connected ])

    def notin(self, ps):
        ps_ids = [ peer.nodeid for peer in ps]
        return PeerSet([ peer for peer in self if peer not in ps_ids])

    def find(self, nodeid):
        if not isinstance(nodeid, int): nodeid = nodeid.nodeid
        peers = [ peer for peer in self if peer.nodeid == nodeid]
        if peers:
            return peers[0]  # Can only be one
        else:
            return None

    def append(self, peer):
        if not isinstance(peer, (list, set)):
            peer = (peer,)
        self.update(peer)

    def debugline(self):
        return str([ p.nodeid for p in self ] )

    def closestto(self, nodeid, exclude = None):
        dist=99999
        closestpeer = None
        excludeids = [ peer.nodeid for peer in exclude ] if exclude else [ ]
        for peer in self:
            if (peer.nodeid not in excludeids) and (peer.distanceto(nodeid) < dist):
                dist = peer.distanceto(nodeid)
                closestpeer = peer
        return closestpeer


    def furthestfrom(self, nodeid):
        dist = 0
        furthestpeer = None
        for peer in self:
            if peer.distanceto(nodeid) > dist:
                dist = peer.distanceto(nodeid)
                furthestpeer = peer
        return furthestpeer

    def __str__(self):
        return str([p.nodeid for p in self])


class Peer(object):
    """
    One for each node we know about.
    Applies to both connected and disconnected peers.
    """
    def __init__(self, node=None, connectedvia=None, nodeid=None, ipaddr=None, **parms):
        self.connected = False   # Start off disconnected
        self.cachedpeers = PeerSet()
        self.connectedvia = connectedvia if connectedvia else PeerSet()      # List of peers connected via.
        self.nodeid = nodeid
        self.ipaddr = ipaddr      # Where it is on the network
        self.distance = node.distance(self)
        self.node=node              # Parent node (in non Sim there would only ever be one)
        assert  node.nodeid not in [ p.nodeid for p in self.connectedvia ], "Shouldnt ever set connectedvia to incude Node"

    def __repr__(self):
        return "Peer(%d)" % self.nodeid

    def __eq__(self, other):
        if not isinstance(other, int):
            other = other.nodeid
        return self.nodeid == other

    def reqpeers(self): # Get a list of their peers from this peer and store in cachedpeers
        return sim.find(self).handlereqpeers()  # Returns a dicarray

    def disconnect(self, connecttheirpeers=False, verbose=False, reason=""):
        """
        Disconnect from this peer
        if connecttheirpeers then consider connecting to any of their peers
        if candidate then only disconnect if we are over limit
        """
        #if debug: print "Disconnecting",self
        # Would disconnect HTTP here
        verbose=True    # xXx remove
        if verbose: print "Node %d disconnecting from %d because %s" % (self.node.nodeid, self.nodeid, reason )
        #sim.debugprint(level=1)
        #1/0
        self.connected=False
        # Remove any connections onwards since can no longer connect to those cachedpeers via this one we are disconnecting
        for cachedpeer in self.cachedpeers:
            cachedpeer.connectedvia.discard(self)
        if connecttheirpeers:
            print "TODO implement disconnect with connecttheirpeers"


    def connect(self):
        self.connected=True
        #TODO - would connect via HTTP here
        self.node.onconnected(self)

    def dict(self):
        return { 'nodeid': self.nodeid, 'ipaddr': self.ipaddr}

    def debugline(self):
        return "Peer %d ip=%d distance=%d connected=%d cachedpeers=%s connectedvia=%s"  \
               % (self.nodeid, self.ipaddr or 0, self.distance, self.connected, self.cachedpeers.debugline(), self.connectedvia.debugline() )

    def distanceto(self, peerid):
        if isinstance(peerid, (Peer,Node)): peerid = peerid.nodeid
        offset = peerid ^ self.nodeid
        return bin(offset).count("1")

    def sendMessage(self, msg):
        # In real code this would send via HTTP, instead it simulates locally by finding the Node in "sim"
        if msg.hops >= msg.maxhops:
            if msg.verbose: print "Max hops exceeded"
            print "XXX@295 max hops exceeded"
            msg.debugprint() # XXX comment out
            1/0
            return PeerResponse(success=False, err="Max hops exceeded", msg=msg)
        return sim.sendMessage(self, msg)        # Ok to simulate sending

    def setcachedpeers(self, node, pdiclist):
        self.cachedpeers = PeerSet()  # Empty list
        for p in pdiclist:
            existingpeer = node.peers.find(p["nodeid"])
            if existingpeer:
                self.cachedpeers.append(existingpeer)
                if self not in (node, existingpeer) and (node != existingpeer):
                    assert self.nodeid != node.nodeid, "Shouldnt be working on node anyway"
                    existingpeer.connectedvia.append(self)
                    existingpeer.ipaddr = p["ipaddr"]
            else:
                cv = (self,) if self not in (node, p["nodeid"]) else []
                newpeer = Peer(node=node, connectvia=PeerSet(cv), **p)
                self.cachedpeers.append(newpeer)
                node.peers.append(newpeer)

    def closer(self, other):
        """
        True if self is closer to its node than other
        """
        return self.node.closer(self, other)

class NodeList(list):
    """
    List of all nodes for simulation
    """
    def find(self, nodeid):
        if isinstance(nodeid, (Peer,Node)): nodeid = nodeid.nodeid
        nodes = [ node for node in self if node.nodeid == nodeid]
        if nodes:
            return nodes[0]  # Can only be one
        else:
            return None

    def debugprint(self, level=2):
        for n in self:
            n.debugprint(level=level)



class Sim(NodeList):
    def __init__(self):
        super(Sim, self).__init__()

    def reset(self):
        """ Reset for new simulation"""
        self.__init__()   # Empty it out

    def createnodes(self, numnodes):
        for n in range(numnodes):
            self.append(Node())

    def createconnections(self, numconnects):
        numnodes = len(self)
        for c in range(numconnects):
            nodefrom = self.randnode()
            connectto = self.randnode()
            if nodefrom != connectto:
                peer = nodefrom.seedpeer(nodeid=connectto)
                if peer:
                    nodefrom.onconnected(peer)

    def randnode(self):
        numnodes = len(self)
        return self[randint(0, numnodes-1)]

    def findroute(self, source, destn, maxhops=0, verbose=False):
        msg = PeerMessage(nodeid = self[destn].nodeid, hops=0, tried=None, verbose=verbose)
        res = self[source].sendMessage(msg)
        if verbose: print "Success",res.success
        #if not res.success: print "XXX@360",res.err
        return res.success


    def countconnections(self, verbose=False):
        ok = 0
        tests = 0
        for source in range(0,len(self)):
            for destn in range(0,len(self)):
                if source != destn:
                    #print "XXX@374 testing", source, destn,
                    tests += 1
                    if self.findroute(source, destn, maxhops=len(self), verbose=verbose):
                        #print "OK"
                        ok += 1
                    else:
                        pass
                        #print "FAIL"
        if verbose: print "%d/%d" % (ok, tests)
        return float(ok) * 100 /tests

    def avgcountconnections(self,nodes, line=False, verbose=False):
        self.createnodes(nodes)
        self.setminmax(int(nodes), int(nodes))
        if not line:
            print "nodes,connections,percent"
        else:
            print "%d," % nodes,
        had100 = False  # Set to True when have first 100
        for i in range(nodes):
            self.createconnections(nodes)
            self.loop(nodes*10)                # Randomly do a bunch of optimisation etc
            percent = sim.countconnections(verbose=verbose)
            if line:
                print "%d," % percent,
            else:
                print "%d %d %d%%" % (nodes, i, percent)
            if percent == 100: # and not had100:    # XXX put back the "and not had100"
                print ">" # End partial line
                sim.debugprint(level=1)
                had100 = True
            elif percent <100 and had100:
                print "<"  # End partial line
                sim.debugprint(level=1)
                1/0
                return
        print "."   # End line

    def loop(self, loops=1):
        for i in range(loops):
            self.randnode().loop()

    def setminmax(self, min, max):
        for node in self:
            node.setminmax(min, max)

    def sendMessage(self, peer, msg):
        """ Simulate a send """
        destnode = self.find(peer)  # Find the node in the simulator
        return destnode.sendMessage(msg.copy())  # Will simulate HTTP call, make copy of message so don't edit in supposedly independant simulation


class PeerMessage(object):
    """
    Overloaded dictionary sent to Peers
    """
    pass

    def __init__(self, nodeid=None, hops=0, tried=None, verbose=False, maxhops=100, payload=None):
        self.nodeid = nodeid
        self.hops = hops
        self.tried = tried or PeerSet() # Initialize if unset
        self.verbose = verbose
        self.payload = payload
        self.maxhops = maxhops

    def copy(self):
        return PeerMessage(nodeid=self.nodeid, hops=self.hops, tried=self.tried.copy(), verbose=self.verbose, maxhops=self.maxhops, payload=self.payload)

    def debugprint(self, level=2):
        print "To: %d Hops=%d maxhops=%d Tried=%s" % (self.nodeid, self.hops, self.maxhops, self.tried)

class PeerResponse(object):
    """
    Overloaded dictionary returned from Peers with answer
    """
    pass

    def __init__(self, err=None, payload=None, msg=None, success=False):
        self.hops = msg.hops
        self.err = err
        self.payload = payload
        self.tried = msg.tried
        self.success = success

sim = Sim()

def median(lst):
    return numpy.median(numpy.array(lst))

def test_generic():
    """
    Workout some of the functionality
    """
    sim.append(Node())  # Create one Node
    sim.append(Node())  # Create second Node
    sim.append(Node())  # Create third Node
    assert sim[0].handlereqpeers() == [],"Should be empty"
    peer01 = sim[0].seedpeer(nodeid=sim[1].nodeid)
    peer20 = sim[2].seedpeer(nodeid=sim[0].nodeid)
    peer21 = sim[2].seedpeer(nodeid=sim[1].nodeid)
    #sim.debugprint(level=2)
    #print "---"
    sim[0].onconnected(peer01)
    #sim.debugprint(level=2)
    #print "---"
    sim[2].onconnected(peer21)
    sim[2].onconnected(peer20)
    sim.debugprint(level=2)
    print "---"

def test_sim1():
    sim.avgcountconnections(200)

def test_sim2():
    nodes = 200
    connections=nodes*10
    loops=nodes*10
    sim.createnodes(nodes)
    sim.setminmax(int(nodes/10), int(nodes/5))
    sim.createconnections(connections)
    sim.loop(loops)
    percent = sim.countconnections()
    print "nodes=%d connections=%d loops=%d percent=%d" % (nodes, connections, loops, percent)

def test_sim3():
    inc = 2
    nodes = 40
    for i in range(1, int(nodes/inc)):
        sim.reset() # Clear Sim back to zero
        sim.avgcountconnections(i * inc, line=True, verbose=False)