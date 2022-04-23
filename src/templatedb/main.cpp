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

int main(int argc, char **argv){ 
  // arg[0]:  it is datafile we are using for queries
  // arg[1]: table size
  // arg[2]: 1 = tiering / 0 = leveling
  // arg[3]: if arg1 = 0 / null
  //       else it is equal to number of files per run
  // 
  if(argc < 3){
    std::cout << "Not enough input arguements" << endl;
  }
  else if(stoi(argv[2]) != 0 || stoi(argv[2])!= 1){
    std::cout << "You have not selected a proper merging policy" << endl;
    return 0;
  }
  else if((stoi(argv[2]) == 1 && argc < 4) || ((stoi(argv[2]) == 0) && argc < 3)){
    std::cout << "The number of arguments is incorrect according to the desired architecture." << std::endl;
    return 0;
  }


    //templatedb::Operation test;
   // cout << test.type;
      cout<< "hello\n";

    templatedb::DB test; // DB(input args)
    test.tablesize = stoi(argv[1]);
    test.tiering = (bool)stoi(argv[2]);
    test.sizeRatio = 1; 
    if(stoi(argv[2]) == 1){
      //test = templatedb::DB(stoi(argv[1]), (bool)stoi(argv[2]), stoi(argv[3]));
      test.sizeRatio = stoi(argv[3]);
      cout<< "hello\n";
    }
   	cout<< "check files were created\n";

   	std::vector<Operation> queries;

   	//queries = templatedb::Operation::ops_from_file("../../data/test_401_3.data");
    queries = templatedb::Operation::ops_from_file(argv[0]);
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