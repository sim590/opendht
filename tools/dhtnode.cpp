/*
 *  Copyright (C) 2014-2015 Savoir-Faire Linux Inc.
 *
 *  Author: Adrien Béraud <adrien.beraud@savoirfairelinux.com>
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  If you modify this program, or any covered work, by linking or
 *  combining it with the OpenSSL project's OpenSSL library (or a
 *  modified version of that library), containing parts covered by the
 *  terms of the OpenSSL or SSLeay licenses, Savoir-Faire Linux Inc.
 *  grants you additional permission to convey the resulting work.
 *  Corresponding Source for a non-source form of such a combination
 *  shall include the source code for the parts of OpenSSL used as well
 *  as that of the covered work.
 */

#include "tools_common.h"
extern "C" {
#include <gnutls/gnutls.h>
}

#include <set>

using namespace dht;

void print_usage() {
    std::cout << "Usage: dhtnode [-p local_port] [-b bootstrap_host]" << std::endl << std::endl;
    std::cout << "dhtnode, a simple OpenDHT command line node runner." << std::endl;
    std::cout << "Report bugs to: http://opendht.net" << std::endl;
}

void print_id_req() {
    std::cout << "An identity is required to perform this operation (run with -i)" << std::endl;
}

void print_node_info(const DhtRunner& dht, const dht_params& params) {
    std::cout << "OpenDht node " << dht.getNodeId() << " running on port " <<  params.port << std::endl;
    if (params.is_bootstrap_node)
        std::cout << "Running in bootstrap mode (discouraged)." << std::endl;
    if (params.generate_identity)
        std::cout << "Public key ID " << dht.getId() << std::endl;
}

int
main(int argc, char **argv)
{
    DhtRunner dht;
    try {
        auto params = parseArgs(argc, argv);
        if (params.help) {
            print_usage();
            return 0;
        }

        // TODO: remove with GnuTLS >= 3.3
        int rc = gnutls_global_init();
        if (rc != GNUTLS_E_SUCCESS)
            throw std::runtime_error(std::string("Error initializing GnuTLS: ")+gnutls_strerror(rc));

        dht::crypto::Identity crt {};
        if (params.generate_identity) {
            auto ca_tmp = dht::crypto::generateIdentity("DHT Node CA");
            crt = dht::crypto::generateIdentity("DHT Node", ca_tmp);
        }
    
        dht.run(params.port, crt, true, params.is_bootstrap_node);

        if (params.log)
            enableLogging(dht);

        if (not params.bootstrap.first.empty()) {
            std::cout << "Bootstrap: " << params.bootstrap.first << ":" << params.bootstrap.second << std::endl;
            dht.bootstrap(params.bootstrap.first.c_str(), params.bootstrap.second.c_str());
        }

        print_node_info(dht, params);
        std::cout << " (type 'h' or 'help' for a list of possible commands)" << std::endl << std::endl;

        while (true)
        {
            std::cout << ">> ";
            std::string line;
            std::getline(std::cin, line);
            std::istringstream iss(line);
            std::string op, idstr, value;
            iss >> op >> idstr;

            if (std::cin.eof() || op == "x" || op == "q" || op == "exit" || op == "quit") {
                break;
            } else if (op == "h" || op == "help") {
                std::cout << "OpenDht command line interface (CLI)" << std::endl;
                std::cout << "Possible commands:" << std::endl;
                std::cout << "  h, help    Print this help message." << std::endl;
                std::cout << "  q, quit    Quit the program." << std::endl;
                std::cout << "  log        Print the full DHT log." << std::endl;

                std::cout << std::endl << "Node information:" << std::endl;
                std::cout << "  ll         Print basic information and stats about the current node." << std::endl;
                std::cout << "  ls         Print basic information about current searches." << std::endl;
                std::cout << "  ld         Print basic information about currenty stored values on this node." << std::endl;
                std::cout << "  lr         Print the full current routing table of this node" << std::endl;

                std::cout << std::endl << "Operations on the DHT:" << std::endl;
                std::cout << "  g [key]               Get values at [key]." << std::endl;
                std::cout << "  l [key]               Listen for value changes at [key]." << std::endl;
                std::cout << "  p [key] [str]         Put string value at [key]." << std::endl;
                std::cout << "  s [key] [str]         Put string value at [key], signed with our generated private key." << std::endl;
                std::cout << "  e [key] [dest] [str]  Put string value at [key], encrypted for [dest] with its public key (if found)." << std::endl;
                std::cout << std::endl;
                continue;
            } else if (op == "ll") {
                print_node_info(dht, params);
                unsigned good4, dubious4, cached4, incoming4;
                unsigned good6, dubious6, cached6, incoming6;
                dht.getNodesStats(AF_INET, &good4, &dubious4, &cached4, &incoming4);
                dht.getNodesStats(AF_INET6, &good6, &dubious6, &cached6, &incoming6);
                std::cout << "IPv4 nodes : " << good4 << " good, " << dubious4 << " dubious, " << incoming4 << " incoming." << std::endl;
                std::cout << "IPv6 nodes : " << good6 << " good, " << dubious6 << " dubious, " << incoming6 << " incoming." << std::endl;
                continue;
            } else if (op == "lr") {
                std::cout << "IPv4 routing table:" << std::endl;
                std::cout << dht.getRoutingTablesLog(AF_INET) << std::endl;
                std::cout << "IPv6 routing table:" << std::endl;
                std::cout << dht.getRoutingTablesLog(AF_INET6) << std::endl;
                continue;
            } else if (op == "ld") {
                std::cout << dht.getStorageLog() << std::endl;
                continue;
            } else if (op == "ls") {
                std::cout << "Searches:" << std::endl;
                std::cout << dht.getSearchesLog() << std::endl;
                continue;
            } else if (op == "la")  {
                std::cout << "Reported public addresses:" << std::endl;
                auto addrs = dht.getPublicAddressStr();
                for (const auto& addr : addrs)
                    std::cout << addr << std::endl;
                continue;
            } else if (op == "log") {
                params.log = !params.log;
                if (params.log)
                    enableLogging(dht);
                else
                    disableLogging(dht);
                continue;
            }

            if (op.empty())
                continue;

            dht::InfoHash id {idstr};
            static const std::set<std::string> VALID_OPS {"g", "l", "p", "s", "e", "a"};
            if (VALID_OPS.find(op) == VALID_OPS.cend()) {
                std::cout << "Unknown command: " << op << std::endl;
                std::cout << " (type 'h' or 'help' for a list of possible commands)" << std::endl;
                continue;
            }
            static constexpr dht::InfoHash INVALID_ID {};
            if (id == INVALID_ID) {
                std::cout << "Syntax error: invalid InfoHash." << std::endl;
                continue;
            }

            auto start = std::chrono::high_resolution_clock::now();
            if (op == "g") {
                dht.get(id, [start](std::shared_ptr<Value> value) {
                    auto now = std::chrono::high_resolution_clock::now();
                    std::cout << "Get: found value (after " << print_dt(now-start) << "s)" << std::endl;
                    std::cout << "\t" << *value << std::endl;
                    return true;
                }, [start](bool ok) {
                    auto end = std::chrono::high_resolution_clock::now();
                    std::cout << "Get: " << (ok ? "completed" : "failure") << " (took " << print_dt(end-start) << "s)" << std::endl;
                });
            }
            else if (op == "l") {
                std::cout << id << std::endl;
                dht.listen(id, [](std::shared_ptr<Value> value) {
                    std::cout << "Listen: found value:" << std::endl;
                    std::cout << "\t" << *value << std::endl;
                    return true;
                });
            }
            else if (op == "p") {
                std::string v;
                iss >> v;
                dht.put(id, dht::Value {
                    dht::ValueType::USER_DATA.id,
                    std::vector<uint8_t> {v.begin(), v.end()}
                }, [start](bool ok) {
                    auto end = std::chrono::high_resolution_clock::now();
                    std::cout << "Put: " << (ok ? "success" : "failure") << " (took " << print_dt(end-start) << "s)" << std::endl;
                });
            }
            else if (op == "s") {
                if (not params.generate_identity) {
                    print_id_req();
                    continue;
                }
                std::string v;
                iss >> v;
                dht.putSigned(id, dht::Value {
                    dht::ValueType::USER_DATA.id,
                    std::vector<uint8_t> {v.begin(), v.end()}
                }, [start](bool ok) {
                    auto end = std::chrono::high_resolution_clock::now();
                    std::cout << "Put signed: " << (ok ? "success" : "failure") << " (took " << print_dt(end-start) << "s)" << std::endl;
                });
            }
            else if (op == "e") {
                if (not params.generate_identity) {
                    print_id_req();
                    continue;
                }
                std::string tostr;
                std::string v;
                iss >> tostr >> v;
                dht.putEncrypted(id, InfoHash(tostr), dht::Value {
                    dht::ValueType::USER_DATA.id,
                    std::vector<uint8_t> {v.begin(), v.end()}
                }, [start](bool ok) {
                    auto end = std::chrono::high_resolution_clock::now();
                    std::cout << "Put encrypted: " << (ok ? "success" : "failure") << " (took " << print_dt(end-start) << "s)" << std::endl;
                });
            }
            else if (op == "a") {
                in_port_t port;
                iss >> port;
                dht.put(id, dht::Value {dht::IpServiceAnnouncement::TYPE.id, dht::IpServiceAnnouncement(port)}, [start](bool ok) {
                    auto end = std::chrono::high_resolution_clock::now();
                    std::cout << "Announce: " << (ok ? "success" : "failure") << " (took " << print_dt(end-start) << "s)" << std::endl;
                });
            }
        }

        std::cout << std::endl <<  "Stopping node..." << std::endl;
    } catch(const std::exception&e) {
        std::cout << std::endl <<  e.what() << std::endl;
    }

    dht.join();
    gnutls_global_deinit();

    return 0;
}
