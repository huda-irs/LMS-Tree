#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "operation.hpp"
#include "db.hpp"
#include "db.cpp"

using namespace std;

int main(){
    //templatedb::Operation test;
   // cout << test.type;

    templatedb::DB test;
    test.newfiles();

    return 0;
}