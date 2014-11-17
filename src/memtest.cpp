#include <iostream>
#include <string>
#include <map>
#include <unordered_map>

#include "meminfo.h"

using namespace std;

int main(int argc, char const *argv[])
{
    int a = 5;
    while (a--) {
    size_t size = 128 * 1024;
    cout << "getCurrentRSS: " << getCurrentRSS() << "\n";
    {
        unordered_map<string, string> cache;
        for (int i = 0; i < size; ++i) {
            cache[to_string(i)] = string(4096, 'A');
        }
        cout << "getCurrentRSS: " << getCurrentRSS() << "\n";

        for (int i = 0; i < size/2; ++i) {
            cache.erase(to_string(i));
        }
        cout << "getCurrentRSS: " << getCurrentRSS() << "\n";
    }
    cout << "getCurrentRSS: " << getCurrentRSS() << "\n";
}
    return 0;
}