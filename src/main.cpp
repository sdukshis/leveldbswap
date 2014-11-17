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

#include "durable_storage.h"

using namespace std;

const size_t MAX_CACHE_SIZE = 64 * 1024 * 1024; // 64 Mb

atomic_bool running(true);

void sighandler(int /* signum */)
{
    running = false;
}

int main(int argc, char const *argv[])
{
    try {
        if (SIG_ERR == signal(SIGINT, sighandler)) {
            perror("signal");
            throw runtime_error(strerror(errno));
        }

        DurableStorage storage("/tmp/testdb", MAX_CACHE_SIZE);

        srand(time(nullptr));
        vector<string> keys;


        // Main loop
        while (running) {
            if (rand() % 2) {
                // Write
                auto key = to_string(rand());
                string value(rand() % 4096, 'A');
                storage.put(key, value);
                keys.push_back(key);                
            } else {
                // Read
                if (keys.size()) {
                    auto key = keys[rand() % keys.size()];
                    string value;
                    storage.get(key, value);
                }
            }
        }
        cout << "Quit\n";
    } catch (const exception &e) {
        cerr << e.what() << endl;
    }
    return 0;
}