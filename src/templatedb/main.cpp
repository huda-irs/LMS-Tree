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
      cout<< "hello\n";

    templatedb::DB test;
    cout<< "hello\n";
    test.newfiles();

   	cout<< "check files were created\n";

   	std::vector<Operation> queries;

   	queries = templatedb::Operation::ops_from_file("../../data/test_10000_3.data");

   	cout << queries.size() << " is the number of operations we have\n";

   	Value prefetchTable[queries.size()];

   	for(int i = 0; i < queries.size(); i++){
   		prefetchTable[i].items = queries[i].args;
   	}

   	for(int i = 0; i < queries.size(); i++){
   		if(queries[i].type == 3){
   			test.put(queries[i].key, prefetchTable[i] );
   		}
      else if(queries[i].type == 2){
        test.del(queries[i].key);
      }
      else if(queries[i].type == 1) {
        test.scan();
      }
      else if(queries[i].type == 0){
        test.get(queries[i].key);
      }
      else if(queries[i].type == 100){
        continue;
      }
      else{
        cout << "This opcode does not fall within the list available for this DB" << endl;
        cout << "That is why this operation will be ignored and will continue with the rest of the queries" << endl;
      }


   	}

    // test.close();

    return 0;
}