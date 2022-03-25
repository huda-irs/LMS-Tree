#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "operation.hpp"
#include "operation.cpp"
#include "db.hpp"
#include "db.cpp"

using namespace std;

int main(){
    //templatedb::Operation test;
   // cout << test.type;
    templatedb::DB test;
    test.newfiles();

   	cout<< "check files were created\n";

   	std::vector<Operation> queries;

   	queries = templatedb::Operation::ops_from_file("../../data/test_150_4.data");

   	cout << queries.size() << " is the number of operations we have\n";

   	Value prefetchTable[queries.size()];

   	for(int i = 0; i < queries.size(); i++){
   		prefetchTable[i].items = queries[i].args;
   	}

   	for(int i = 0; i < queries.size(); i++){
   		if(queries[i].type == 3){
   			test.put(queries[i].key,prefetchTable[i] );
   		}

   	}

    return 0;
}