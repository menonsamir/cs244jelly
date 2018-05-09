import random
from collections import deque
def build_graph(n_switch, n_interswitch_ports, n_server_oports, n_servers):
    graph = dict()
    for i in range(n_switch):
        graph[i] = set()
    switches_with_free_port = range(n_switch)

    def connect_switches(s1,s2):
        graph[s1].add(s2)
        if len(graph[s1]) == n_interswitch_ports:
            switches_with_free_port.remove(s1)
        graph[s2].add(s1)
        if len(graph[s2]) == n_interswitch_ports:
            switches_with_free_port.remove(s2)

    def done_connecting():
        for si in switches_with_free_port:
            for sj in switches_with_free_port:
                if si != sj:
                    if si not in graph[sj]:
                        return False
        return True

    while True:
        print("outer")
        while True:
            #print(len(switches_with_free_port))
            if len(switches_with_free_port) < 2:
                break
            if done_connecting():
                break
            pair = random.sample(switches_with_free_port, 2)
            s1 = pair[0]
            s2 = pair[1]
            if s2 not in graph[s1]:
                connect_switches(s1, s2)
        sfp = len(switches_with_free_port)
        #if sfp == 0 or ((sfp == 1) and (len(graph[switches_with_free_port[0]]) == n_interswitch_ports-1)):
        #    break # we done
        if sfp < 2:
            break
        
        # get switch with two remaining free ports (p1,p2) - if it only has 1, just drop an existing
        s = switches_with_free_port[0]
        assert(len(graph[s]) > 0)
        if len(graph[s]) == 1:
            drop_target = random.choice(list(graph[s]))
            graph[s].remove(drop_target)
            graph[drop_target].remove(s)
            if drop_target not in switches_with_free_port:
                switches_with_free_port.append(drop_target)
        p1 = graph[s].pop()
        p2 = graph[s].pop()
        # remove random link (x,y)
        x=0
        y=0
        while x == y or y not in graph[x] or x==p1 or x==p2 or y==p1 or y==p2:
            x = random.randint(0, n_switch-1)
            y = random.randint(0, n_switch-1)
        graph[x].remove(y)
        graph[y].remove(x)
        # add (p1,x) and (p2,y)
        graph[p1].add(x)
        graph[x].add(p1)
        graph[p2].add(y)
        graph[y].add(p2)
    return graph

def add_servers(n_switch, n_server_oports, n_servers):
    switch_of = dict()
    switch_to_server = dict()
    for i in range(n_switch):
        switch_to_server[i] = set()
        
    for i in range(n_servers):
        sw = random.randint(0,n_switch-1)
        while len(switch_to_server[sw]) == n_server_oports:
            sw = random.randint(0,n_switch-1)
        switch_of[i] = sw
        switch_to_server[sw].add(i)
    return switch_of, switch_to_server

def get_paths(perm, graph, switch_of, k, n_servers, kind):
    def k_shortest_paths(source, dest):
        source = switch_of[source]
        dest = switch_of[dest]
        paths = []
        queue = deque()
        queue.append([source])
        while len(paths) < k and len(queue) > 0:
            path = queue.popleft()
            end = path[-1]
            if end == dest:
                paths.append(path)
            else:
                for neighbor in graph[end] - set(path):
                    newpath = path + [neighbor]
                    queue.append(newpath)
        return paths

    def do_ecmp(send, recv):
        paths = k_shortest_paths(send, recv)
        if len(paths) == 0:
            print "hmmm"
            return []
        min_len = len(paths[0])
        ecmp_paths = []
        i = 0
        while i < len(paths) and len(paths[i]) == min_len:
            ecmp_paths.append(paths[i])
            i += 1
        return ecmp_paths

    def do_it(send, recv):
        paths = []
        if kind == "kshortest":
            paths = k_shortest_paths(send, recv)
        else:
            paths = do_ecmp(send, recv)
        all_paths[(send,recv)] = paths

    all_paths = dict()
    servers = perm
    send_recv_pairs = []
    for i in range(n_servers-1):
        send_recv_pairs.append((servers[i], servers[i+1]))
        do_it(servers[i], servers[i+1])
    do_it(servers[-1], servers[0])

    return all_paths