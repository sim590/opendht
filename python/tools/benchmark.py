#!/usr/bin/env python3
# Copyright (C) 2015 Savoir-Faire Linux Inc.
# Author: Adrien Béraud <adrien.beraud@savoirfairelinux.com>

import os, sys, subprocess, argparse, time, random, string, threading, queue, signal
from pyroute2.netns.process.proxy import NSPopen
import numpy as np
import matplotlib.pyplot as plt
from dhtnetwork import DhtNetwork

sys.path.append('..')
from opendht import *

def random_hash():
    return PyInfoHash(''.join(random.SystemRandom().choice(string.hexdigits) for _ in range(40)).encode())

def start_cluster(i):
    global procs
    cmd = ["python3", "dhtnetwork.py", "-n", str(node_per_loc), '-I', args.ifname+str(i)+'.1']
    if not args.disable_ipv4 and bootstrap.ip4:
        cmd.extend(['-b', bootstrap.ip4])
    if not args.disable_ipv6 and bootstrap.ip6:
        cmd.extend(['-b6', bootstrap.ip6])
    procs[i] = NSPopen('node'+str(i), cmd)
    plt.pause(2)

def stop_cluster(i):
    global procs
    if procs[i]:
        try:
            procs[i].send_signal(signal.SIGINT);
            procs[i].wait()
            procs[i].release()
        except Exception as e:
            print(e)
        procs[i] = None

def replace_cluster():
    n = random.randrange(0, clusters)
    stop_cluster(n)
    start_cluster(n)

#TODO: Test this
def dataPersistenceTest():
    """TODO: Docstring for dataPersistenceTest.

    """
    DEL_REQ = b"del"
    BENCHMARK_FIFO = 'bm_fifo'

    fifo_lock = threading.Condition()
    lock      = threading.Condition()
    done      = 0

    foreign_nodes = []
    foreign_values = []

    def benchmark_notify():
        nonlocal BENCHMARK_FIFO, fifo_lock

        with open(BENCHMARK_FIFO, 'r') as fifo:
            for line in iter(fifo.readline, b''):
                if not line:
                    time.sleep(sleep_time)
                    if sleep_time < 1.0:
                        sleep_time += 0.00001
                    continue
                sleep_time = 0.00001
                with fifo_lock:
                    fifo_lock.notify()

    def getcb(value):
        nonlocal foreign_values
        print('[GET]: %s' % value)
        foreign_values.append(value)

    def putDoneCb(ok, nodes):
        nonlocal lock, done
        with lock:
            done -= 1
            lock.notify()

    def getDoneCb(ok, nodes):
        nonlocal lock, done, foreign_nodes
        with lock:
            if not ok:
                print("[GET]: failed !")
            else:
                for node in nodes:
                    foreign_nodes.append(node.getId())
                print('[GET] hosts nodes: %s ' % nodes)
            done -= 1
            lock.notify()

    t = threading.Thread(target=benchmark_notify)

    try:
        try:
            os.mkfifo(BENCHMARK_FIFO)
        except Exception:
            pass
        t.start()

        myhash = random_hash()
        #localvalues = [PyValue(b'foo'), PyValue(b'bar'), PyValue(b'foobar')]
        localvalues = [PyValue(b'foo')]
        successfullTransfer = lambda lv,fv: len(lv) == len(fv)

        for val in localvalues:
            with lock:
                print('[PUT]: %s' % val)
                done += 1
                bootstrap.front().put(myhash, val, putDoneCb)
                while done > 0:
                    lock.wait()

        print('Waiting 10 seconds for the network to do its thing...')
        time.sleep(10)

        # checking if values were transfered.
        with lock:
            done += 1
            bootstrap.front().get(myhash, getcb, getDoneCb)
            while done > 0:
                lock.wait()

        if not successfullTransfer(localvalues, foreign_values):
            print('[GET]: Only %s on %s values successfully put.' %
                    (len(foreign_values), len(localvalues)))
        if foreign_values and foreign_nodes:
            print('Removing all nodes hosting target values...')
            serialized_req = DEL_REQ + b" " + b" ".join(map(bytes, foreign_nodes))
            for proc in procs:
                with fifo_lock:
                    print('[REMOVE]: sending (req: "%s") to %s' %
                            (serialized_req, proc))
                    print(proc.stdin)
                    proc.stdin.write(serialized_req + b'\n')
                    proc.stdin.flush()
                    #counting on fifo to wake up.
                    print('[benchmark] going to wait()')
                    fifo_lock.wait()
                    print('[benchmark]: got notify')


            # checking if values were transfered to new nodes
            foreign_values = []
            with lock:
                print('[GET]: trying to fetch persistant values')
                done += 1
                bootstrap.front().get(myhash, getcb, getDoneCb)
                while done > 0:
                    lock.wait()

            if not successfullTransfer(localvalues, foreign_values):
                print('[GET]: Only %s on %s values persisted.' %
                        (len(foreign_values), len(localvalues)))
            else:
                print('[GET]: All values successfully persisted.')
        else:
            print("[GET]: either couldn't fetch values or nodes hosting values...")

    except Exception as e:
        print(e)
    finally:
        os.remove(BENCHMARK_FIFO)

def getsTimesTest():
    """TODO: Docstring for

    """

    plt.ion()

    lines = plt.plot([])
    plt.ylabel('time (s)')
    #plt.show()

    # let the network stabilise
    plt.pause(5)

    start = time.time()
    times = []

    lock = threading.Condition()
    done = 0

    def getcb(v):
        print("found", v)

    def donecb(ok, nodes):
        nonlocal lock, done, times
        t = time.time()-start
        with lock:
            if not ok:
                print("failed !")
            times.append(t)
            done -= 1
            lock.notify()

    def update_plot():
        nonlocal lines
        while lines:
            l = lines.pop()
            l.remove()
            del l
        lines = plt.plot(times, color='blue')
        plt.draw()

    plt.pause(5)

    plt.show()
    update_plot()

    times = []
    for n in range(10):
        #replace_cluster()
        plt.pause(2)
        print("Getting 50 random hashes succesively.")
        for i in range(50):
            with lock:
                done += 1
                start = time.time()
                bootstrap.front().get(random_hash(), getcb, donecb)
                while done > 0:
                    lock.wait()
            update_plot()
        print("Took", np.sum(times), "mean", np.mean(times), "std", np.std(times), "min", np.min(times), "max", np.max(times))

    print('GET calls timings benchmark test : DONE. '  \
            'Close Matplotlib window for terminating the program.')
    plt.ioff()
    plt.show()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create a dummy network interface for testing')
    parser.add_argument('-i', '--ifname', help='interface name', default='ethdht')
    parser.add_argument('-n', '--node-num', help='number of dht nodes to run', type=int, default=32)
    parser.add_argument('-v', '--virtual-locs', help='number of virtual locations (node clusters)', type=int, default=8)
    parser.add_argument('-l', '--loss', help='simulated cluster packet loss (percent)', type=int, default=0)
    parser.add_argument('-d', '--delay', help='simulated cluster latency (ms)', type=int, default=0)
    parser.add_argument('-no4', '--disable-ipv4', help='Enable IPv4', action="store_true")
    parser.add_argument('-no6', '--disable-ipv6', help='Enable IPv6', action="store_true")
    parser.add_argument('--gets', action='store_true', help='Launches get calls timings benchmark test.', default=0)
    parser.add_argument('--data-persistence', action='store_true', help='Launches data persistence benchmark test.', default=0)

    args = parser.parse_args()

    if args.data_persistence + args.gets < 1:
        print('No test specified... Quitting.', file=sys.stderr)
        sys.exit(1)

    clusters = min(args.virtual_locs, args.node_num)
    node_per_loc = int(args.node_num / clusters)

    print("Launching", args.node_num, "nodes (", clusters, "clusters of", node_per_loc, "nodes)")

    if args.virtual_locs > 1:
        cmd = ["/usr/bin/sudo", "python3", "dummy_if.py", "-i", args.ifname, "-n", str(clusters), '-l', str(args.loss), '-d', str(args.delay)]
        if not args.disable_ipv4:
            cmd.append('-4')
        if not args.disable_ipv6:
            cmd.append('-6')
        print(cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        output, err = p.communicate()
        print(output.decode())

    bootstrap = DhtNetwork(iface='br'+args.ifname)
    bootstrap.resize(1)

    procs = [None for _ in range(clusters)]

    try:
        for i in range(clusters):
            start_cluster(i)

        if args.gets:
            getsTimesTest()
        elif args.data_persistence:
            dataPersistenceTest()

    except Exception as e:
        print(e)
    finally:
        for i in range(clusters):
            if procs[i]:
                procs[i].send_signal(signal.SIGINT);
        bootstrap.resize(0)
        print('Removing dummy interfaces...')
        subprocess.call(["/usr/bin/sudo", "python3", "dummy_if.py", "-i", args.ifname, "-n", str(clusters), "-r"])
        for i in range(clusters):
            if procs[i]:
                try:
                    procs[i].wait()
                    procs[i].release()
                except Exception as e:
                    print(e)
