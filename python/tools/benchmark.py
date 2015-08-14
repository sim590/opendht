#!/usr/bin/env python3
# Copyright (C) 2015 Savoir-Faire Linux Inc.
# Author: Adrien Béraud <adrien.beraud@savoirfairelinux.com>

import os
import sys
import subprocess
import time
import random
import string
import threading
import queue
import signal
import argparse

from pyroute2.netns.process.proxy import NSPopen
import numpy as np
import matplotlib.pyplot as plt

from dhtnetwork import DhtNetwork
sys.path.append('..')
from opendht import *

class WorkBench():
    """docstring for WorkBench"""
    def __init__(self, ifname='ethdht', virtual_locs=8, node_num=32, remote_bootstrap=None, loss=0, delay=0, disable_ipv4=False,
            disable_ipv6=False):
        self.ifname       = ifname
        self.virtual_locs = virtual_locs
        self.node_num     = node_num
        self.clusters     = min(virtual_locs, node_num)
        self.node_per_loc = int(self.node_num / self.clusters)
        self.loss         = loss
        self.delay        = delay
        self.disable_ipv4 = disable_ipv4
        self.disable_ipv6 = disable_ipv6

        self.remote_bootstrap = remote_bootstrap
        self.local_bootstrap  = None
        self.procs            = [None for _ in range(self.clusters)]

    def get_bootstrap(self):
        if not self.local_bootstrap:
            self.local_bootstrap = DhtNetwork(iface='br'+self.ifname,
                    first_bootstrap=False if self.remote_bootstrap else True,
                    bootstrap=[(self.remote_bootstrap, "5000")] if self.remote_bootstrap else [])
        return self.local_bootstrap

    def create_virtual_net(self):
        if self.virtual_locs > 1:
            cmd = ["python3", "virtual_network_builder.py", "-i", self.ifname, "-n", str(self.clusters), '-l', str(self.loss), '-d', str(self.delay)]
            if not self.disable_ipv4:
                cmd.append('-4')
            if not self.disable_ipv6:
                cmd.append('-6')
            print(cmd)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            output, err = p.communicate()
            print(output.decode())

    def destroy_virtual_net(self):
        print('Shuting down the virtual IP network.')
        subprocess.call(["python3", "virtual_network_builder.py", "-i", self.ifname, "-n", str(self.clusters), "-r"])

    def start_cluster(self, i):
        if self.local_bootstrap:
            cmd = ["python3", "dhtnetwork.py", "-n", str(self.node_per_loc), '-I', self.ifname+str(i)+'.1']
            if self.remote_bootstrap:
                cmd.extend(['-b', self.remote_bootstrap, '-bp', "5000"])
            else:
                if not self.disable_ipv4 and self.local_bootstrap.ip4:
                    cmd.extend(['-b', self.local_bootstrap.ip4])
                if not self.disable_ipv6 and self.local_bootstrap.ip6:
                    cmd.extend(['-b6', self.local_bootstrap.ip6])
            self.procs[i] = DhtNetworkSubProcess('node'+str(i), cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            while DhtNetworkSubProcess.NOTIFY_TOKEN not in self.procs[i].getline():
                # waiting for process to spawn
                time.sleep(0.5)
        else:
            raise Exception('First create bootstrap.')

    def stop_cluster(self, i):
        if self.procs[i]:
            try:
                self.procs[i].quit()
            except Exception as e:
                print(e)
            self.procs[i] = None

    def replace_cluster(self):
        n = random.randrange(0, self.clusters)
        self.stop_cluster(n)
        self.start_cluster(n)


class DhtNetworkSubProcess(NSPopen):
    """
    Handles communication with DhtNetwork sub process.

    When instanciated, the object's thread is started and will read the sub
    process' stdout until it finds 'DhtNetworkSubProcess.NOTIFY_TOKEN' token,
    therefor, waits for the sub process to spawn.
    """
    NOTIFY_TOKEN = 'notify'

    def __init__(self, ns, cmd, quit=False, **kwargs):
        super(DhtNetworkSubProcess, self).__init__(ns, cmd, **kwargs)
        self._setStdoutFlags()
        self._virtual_ns = ns

        self._quit = quit
        self._lock = threading.Condition()
        self._in_queue = queue.Queue()
        self._out_queue = queue.Queue()

        # starting thread
        self._thread = threading.Thread(target=self._communicate)
        self._thread.daemon = True
        self._thread.start()

    def __repr__(self):
        return 'DhtNetwork on virtual namespace "%s"' % self._virtual_ns
    
    def _setStdoutFlags(self):
        """
        Sets non-blocking read flags for subprocess stdout file descriptor.
        """
        import fcntl
        flags = self.stdout.fcntl(fcntl.F_GETFL)
        self.stdout.fcntl(fcntl.F_SETFL, flags | os.O_NDELAY)

    def _communicate(self):
        """
        Communication thread. This reads and writes to the sub process.
        """
        ENCODING = 'utf-8'
        sleep_time = 0.1
        stdin_line, stdout_line = '', ''

        # first read of process living. Expecting NOTIFY_TOKEN
        while DhtNetworkSubProcess.NOTIFY_TOKEN not in stdout_line:
            stdout_line = self.stdout.readline().decode()
            time.sleep(sleep_time)

        with self._lock:
            self._out_queue.put(stdout_line)

        while not self._quit:
            with self._lock:
                try:
                    stdin_line = self._in_queue.get_nowait()

                    # sending data to sub process
                    self.stdin.write(stdin_line if isinstance(stdin_line, bytes) else
                            bytes(str(stdin_line), encoding=ENCODING))
                    self.stdin.flush()
                except queue.Empty:
                    #waiting for next stdin req to send
                    self._lock.wait(timeout=sleep_time)

            # reading response from sub process
            for stdout_line in iter(self.stdout.readline, b''):
                stdout_line = stdout_line.decode().replace('\n', '')
                if stdout_line:
                    with self._lock:
                        self._out_queue.put(stdout_line)

        with self._lock:
            self._lock.notify()

    def stop_communicating(self):
        """
        Stops the I/O thread from communicating with the subprocess.
        """
        if not self._quit:
            self._quit = True
            with self._lock:
                self._lock.notify()
                self._lock.wait()

    def quit(self):
        """
        Notifies thread and sub process to terminate. This is blocking call
        until the sub process finishes.
        """
        self.stop_communicating()
        self.send_signal(signal.SIGINT);
        self.wait()
        self.release()

    def send(self, msg):
        """
        Send data to sub process.
        """
        with self._lock:
            self._in_queue.put(msg)
            self._lock.notify()

    def getline(self):
        """
        Read line from sub process.

        @return:  A line on sub process' stdout.
        @rtype :  str
        """
        line = ''
        with self._lock:
            try:
                line = self._out_queue.get_nowait()
            except queue.Empty:
                pass
        return line

def random_hash():
    return PyInfoHash(''.join(random.SystemRandom().choice(string.hexdigits) for _ in range(40)).encode())

#TODO: Test this
def dataPersistenceTest():
    """TODO: Docstring for dataPersistenceTest.

    """
    global wb
    bootstrap = wb.get_bootstrap()
    procs = wb.procs

    DEL_REQ = b"del"

    lock = threading.Condition()
    done = 0

    foreign_nodes = []
    foreign_values = []

    def getcb(value):
        nonlocal bootstrap, foreign_values
        bootstrap.log('[GET]: %s' % value)
        foreign_values.append(value)
        return True

    def putDoneCb(ok, nodes):
        nonlocal lock, done
        with lock:
            done -= 1
            lock.notify()

    def getDoneCb(ok, nodes):
        nonlocal bootstrap, lock, done, foreign_nodes
        with lock:
            if not ok:
                bootstrap.log("[GET]: failed !")
            else:
                for node in nodes:
                    if not node.getNode().isExpired():
                        foreign_nodes.append(node.getId().toString())
            done -= 1
            lock.notify()

    try:
        bootstrap.resize(3)
        consumer = bootstrap.get(1)
        producer = bootstrap.get(2)

        myhash = random_hash()
        local_values = [PyValue(b'foo'), PyValue(b'bar'), PyValue(b'foobar')]
        successfullTransfer = lambda lv,fv: len(lv) == len(fv)

        for val in local_values:
            with lock:
                bootstrap.log('[PUT]: %s' % val)
                done += 1
                producer.put(myhash, val, putDoneCb)
                while done > 0:
                    lock.wait()

        # checking if values were transfered.
        with lock:
            done += 1
            consumer.get(myhash, getcb, getDoneCb)
            while done > 0:
                lock.wait()

        if not successfullTransfer(local_values, foreign_values):
            bootstrap.log('[GET]: Only ', len(foreign_values) ,' on ',
                    len(local_values), ' values successfully put.')

        if foreign_values and foreign_nodes:
            bootstrap.log('Values are found on :')
            for node in foreign_nodes:
                bootstrap.log(node)

            bootstrap.log('Removing all nodes hosting target values...')
            serialized_req = DEL_REQ + b' ' + b' '.join(map(bytes, foreign_nodes))
            for proc in procs:
                bootstrap.log('[REMOVE]: sending (req: "', serialized_req, '")',
                        'to', proc)
                proc.send(serialized_req + b'\n')
                while True:
                    out = proc.getline()
                    if DhtNetworkSubProcess.NOTIFY_TOKEN in out:
                        break
                    elif out:
                        # print subprocess stdout's data. This may be the dht
                        # node.
                        DhtNetwork.log(out)
                    else:
                        time.sleep(0.1)

            # checking if values were transfered to new nodes
            foreign_nodes_before_delete = foreign_nodes
            foreign_nodes = []
            foreign_values = []
            with lock:
                bootstrap.log('[GET]: trying to fetch persistant values')
                done += 1
                consumer.get(myhash, getcb, getDoneCb)
                while done > 0:
                    lock.wait()

            if not successfullTransfer(local_values, foreign_values):
                bootstrap.log('[GET]: Only %s on %s values persisted.' %
                        (len(foreign_values), len(local_values)))
            else:
                bootstrap.log('[GET]: All values successfully persisted.')
            if foreign_values and foreign_nodes:
                bootstrap.log('Values are now found on :')
                for node in set(foreign_nodes) - set(foreign_nodes_before_delete):
                    bootstrap.log(node)
        else:
            bootstrap.log("[GET]: either couldn't fetch values or nodes hosting values...")

    except Exception as e:
        print(e)
    finally:
        bootstrap.resize(1)

def getsTimesTest():
    """TODO: Docstring for

    """
    global wb
    bootstrap = wb.get_bootstrap()

    plt.ion()

    fig, axes = plt.subplots(2, 1)
    fig.tight_layout()

    lax = axes[0]
    hax = axes[1]

    lines = None#ax.plot([])
    #plt.ylabel('time (s)')
    hax.set_ylim(0, 2)

    # let the network stabilise
    plt.pause(60)

    #start = time.time()
    times = []

    lock = threading.Condition()
    done = 0

    def getcb(v):
        nonlocal bootstrap
        bootstrap.log("found", v)
        return True

    def donecb(ok, nodes):
        nonlocal bootstrap, lock, done, times
        t = time.time()-start
        with lock:
            if not ok:
                bootstrap.log("failed !")
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

    def run_get():
        nonlocal bootstrap, done
        done += 1
        start = time.time()
        bootstrap.front().get(InfoHash.getRandom(), getcb, lambda ok, nodes: donecb(ok, nodes, start))

    plt.pause(5)

    plt.show()
    update_plot()

    times = []
    for n in range(10):
        wb.replace_cluster()
        plt.pause(2)
        bootstrap.log("Getting 50 random hashes succesively.")
        for i in range(50):
            with lock:
                done += 1
                start = time.time()
                bootstrap.front().get(InfoHash.getRandom(), getcb, donecb)
                while done > 0:
                    lock.wait()
                    update_plot()
            update_plot()
        print("Took", np.sum(times), "mean", np.mean(times), "std", np.std(times), "min", np.min(times), "max", np.max(times))

    print('GET calls timings benchmark test : DONE. '  \
            'Close Matplotlib window for terminating the program.')
    plt.ioff()
    plt.show()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run, test and benchmark a DHT network on a local virtual network with simulated packet loss and latency.')
    parser.add_argument('-i', '--ifname', help='interface name', default='ethdht')
    parser.add_argument('-n', '--node-num', help='number of dht nodes to run', type=int, default=32)
    parser.add_argument('-v', '--virtual-locs', help='number of virtual locations (node clusters)', type=int, default=8)
    parser.add_argument('-l', '--loss', help='simulated cluster packet loss (percent)', type=int, default=0)
    parser.add_argument('-d', '--delay', help='simulated cluster latency (ms)', type=int, default=0)
    parser.add_argument('-b', '--bootstrap', help='Bootstrap node to use (if any)', default=None)
    parser.add_argument('-no4', '--disable-ipv4', help='Enable IPv4', action="store_true")
    parser.add_argument('-no6', '--disable-ipv6', help='Enable IPv6', action="store_true")
    parser.add_argument('--gets', action='store_true', help='Launches get calls timings benchmark test.', default=0)
    parser.add_argument('--data-persistence', action='store_true', help='Launches data persistence benchmark test.', default=0)

    args = parser.parse_args()

    if args.data_persistence + args.gets < 1:
        print('No test specified... Quitting.', file=sys.stderr)
        sys.exit(1)

    wb = WorkBench(args.ifname, args.virtual_locs, args.node_num, loss=args.loss,
            delay=args.delay, disable_ipv4=args.disable_ipv4,
            disable_ipv6=args.disable_ipv6)
    wb.create_virtual_net()

    bootstrap = wb.get_bootstrap()
    bootstrap.resize(1)
    print("Launching", wb.node_num, "nodes (", wb.clusters, "clusters of", wb.node_per_loc, "nodes)")

    try:
        for i in range(wb.clusters):
            wb.start_cluster(i)

        if args.gets:
            getsTimesTest()
        elif args.data_persistence:
            dataPersistenceTest()

    except Exception as e:
        print(e)
    finally:
        for p in wb.procs:
            if p:
                p.quit()
        bootstrap.resize(0)
        sys.stdout.write('Shuting down the virtual IP network... ')
        sys.stdout.flush()
        wb.destroy_virtual_net()
        print('Done.')
