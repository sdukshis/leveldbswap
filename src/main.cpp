#include <cassert>
#include <iostream>
#include <string>
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <atomic>
#include <csignal>
#include <cstring>
#include <sstream>
#include <chrono>

#include <getopt.h>

#include "durable_storage.h"
#include "meminfo.h"

using namespace std;

atomic_bool running(true);

void sighandler(int /* signum */)
{
    running = false;
}

void help(const char *name)
{
    cout << "LevelDB with cache tester\n"
         << "\n"
         << "Usage: " << name << " [options] <dbpath>\n"
         << "\n"
         << "options:\n"
         << "\t-m cache memory size (default = 64 Mb)\n";
}

int main(int argc, char *argv[])
{
    try {
        size_t max_cache_size = 64 * 1024 * 1024; // 64 Mb
        string dbpath;

        int c;
        while ((c = getopt(argc, argv, "hm:")) != -1) {
            switch (c) {
                case 'm':
                    max_cache_size = stoi(optarg) * 1024 * 1024;
                    break;
                case 'h':
                    help(argv[0]);
                    return 0;
                case ':':
                case '?':
                    if (optopt == 'o') {
                        ostringstream ss;
                        ss << "Option '" << optopt << "' "
                           << "required an argument"; 
                        throw invalid_argument(ss.str());
                    }
                    break;
                default:
                    help(argv[0]);
            }
        }

        if (optind == argc) {
            throw invalid_argument("dbpath must be a valid path");
        } else {
            dbpath = argv[optind];
        }

        if (SIG_ERR == signal(SIGINT, sighandler)) {
            perror("signal");
            throw runtime_error(strerror(errno));
        }

        DurableStorage storage("/tmp/testdb", max_cache_size);

        srand(time(nullptr));
        vector<string> keys;

        auto last_stat = std::chrono::system_clock::now();
        auto last_rss = getCurrentRSS();
        // Main loop
        while (running) {
            if (rand() % 10) {
                if (rand() % 6) {
                    // Read
                    if (!keys.empty()) {
                        auto key = keys[rand() % keys.size()];
                        string value;
                        storage.get(key, value);
                    }      
                } else {
                    // Write
                    auto key = to_string(rand());
                    string value(4096, 'A');
                    storage.put(key, value);
                    keys.push_back(key);     
                }              
            } else {
                if (!keys.empty()) {
                    auto pos = keys.begin() + rand() % keys.size();
                    storage.erase(*pos);
                    keys.erase(pos);
                }
            }
            auto now = std::chrono::system_clock::now();
            auto elapsed = std::chrono::duration_cast<chrono::seconds>(now - last_stat);
            if (elapsed.count()) {
                storage.stats();
                last_stat = now;
                cout << getCurrentRSS() << endl;
            }
            // if (!(rand() % 1000)) {
            //     cout << "currentRSS: " << getCurrentRSS()/1024/1024
            //          << ", peakRSS: " << getPeakRSS()/1024/1024 
            //          << ", keys_count: " << keys.size() << endl;
            // }
        }
        cout << "Quit\n";
    } catch (const exception &e) {
        cerr << e.what() << endl;
    }
    return 0;
}