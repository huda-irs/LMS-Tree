#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <sys/stat.h>
#include <algorithm>
#include <stdio.h>
#include "operation.hpp"
#include "operation.cpp"
#include "db.hpp"
#include "db.cpp"

using namespace std;

int main(int argc, char **argv)
{
  // arg[0]: mainFile
  // arg[1] it is datafile we are using for queries
  // arg[2]: table size
  // arg[3]: 1 = tiering / 0 = leveling
  // arg[4]: if arg1 = 0 / null
  //       else it is equal to number of files per run
  //

  std::cout << argv[1] << endl;
  std::cout << argv[2] << endl;
  std::cout << (argv[3]) << endl;
  std::cout << (argv[4]) << endl;

  if (argc < 5)
  {
    std::cout << "Not enough input arguements" << endl;
    return 0;
  }
  else if (stoi(argv[3]) != 0 && stoi(argv[3]) != 1)
  {
    std::cout << "You have not selected a proper merging policy" << endl;
    return 0;
  }

  
  cout << "hello\n";

  

  std::string tiering = stoi(argv[3]) == 1 ? "T" : "L";
  templatedb::DB test; // DB(input args)
  test.tablesize = stoi(argv[2]);
  test.tiering = (bool)stoi(argv[3]);
  test.sizeRatio = stoi(argv[4]);

  int dim = 0;
  ifstream wlFile(argv[1]);
    if (wlFile.is_open())
    { 
      std::string line;
      getline(wlFile, line);
    
      std::stringstream linestream(line); // read header of workload file
      std::string item; // define variable for grabbing data

      std::getline(linestream, item, ' '); // this reads number of requests
      std::getline(linestream, item, ' '); // this reads the dimension number
      dim = stoi(item);
    }

  test.value_dimensions = dim;
  

  cout << "check files were created\n";

  std::vector<Operation> queries;

  // parsing through workload file such that it becomes a vector of type OPERATION (as descripbed in Operation.hpp)
  queries = templatedb::Operation::ops_from_file(argv[1]);
  cout << queries.size() << " is the number of operations we have\n";

  // creating our LSM tree that is specific to merging policy (Leveling/Teiring), table size, size ratio, dimension of data per query
  // by creating a directory where its files will live
  string meta_tree = tiering + "_" + argv[2] + "_" + argv[4] + "_" + std::to_string(dim);
  cout << meta_tree << endl;
  // if directory already exists
  if (mkdir(meta_tree.c_str(), 0777) != 0)
  {
  	// read the meta data of the LSM tree to pick up from its previous state
    ifstream readFile(meta_tree + "/meta_Data");
    if (readFile.is_open())
    {
      std::string line;
      while (getline(readFile, line))
      {
        std::stringstream linestream(line);
        std::string item;
        templatedb::Levels temp;
        test.levelfiles.push_back(temp);

        std::getline(linestream, item, ' ');
        int levelNum = stoi(item);
 
        std::getline(linestream, item, ' ');
        test.levelfiles[levelNum].numFiles = stoi(item);

        std::getline(linestream, item, ' ');
        test.levelfiles[levelNum].fileSize = stoi(item);

        std::getline(linestream, item, ' ');
        test.levelfiles[levelNum].numFilesCap = stoi(item);

        std::vector<string> items;
        while (std::getline(linestream, item, ' '))
        {
          test.levelfiles[levelNum].fileNames.push_back((item));
        }
        }
        for (int i = 0; i < test.levelfiles.size(); i++)
        {
          std::cout << "Level " << i << std::endl;
          for (auto levelFile : test.levelfiles[i].fileNames) 
          {
            std::cout << "Imported: " << levelFile << std::endl;
          }  
      }
    }
    else
    {

      cout << "the directory and its meta data could not be found" << endl;
    }

  }  
  
  cout << meta_tree << endl;
  // this attribute is to allow for dynamic file creation, writing, and reading in correct directory
  test.dirName = meta_tree; 
  cout << "Query size is " << queries.size() << endl;
  Value prefetchTable[queries.size()]; 
  
  // transfering required information from OPERATION type (from Operation.hpp) to VALUE type (from db.hpp)
  for (int i = 0; i < queries.size(); i++)
  {
    // cout << queries[i].type << endl;
    prefetchTable[i].items = queries[i].args;
  }


  // executing workload one by one
  for (int i = 0; i < queries.size(); i++)
  { cout << "Query type being executed is: " << queries[i].type << endl;
    if (queries[i].type == 3)
    {
      test.put(queries[i].key, prefetchTable[i]);
    }
    else if (queries[i].type == 2)
    {	
    	if(prefetchTable[i].items.size() > 0){ // in the case of range delete, the delete function will be called for x number of times (min_key-max_key +1)
    		
        cout << "Delete min key: " << queries[i].key << endl;
        cout << "Delete max key: " << prefetchTable[i].items[0] << endl;

        int steps = prefetchTable[i].items[0] - queries[i].key  + 1;
        int del_key =  queries[i].key;
        for( int i = 0 ; i < steps ; i ++){ // set i equal to the key value we want to delete
    			cout << "Doing range delete, about to delete key# " << i << endl;
          test.del(del_key + i);
    		}
    	}

		else{ // single key needs to get deleted therefore we can directly assign the key value
			  test.del(queries[i].key);
		}
    }
    else if (queries[i].type == 1)
    { 
      cout << queries[i].args.size() << endl;
      if(prefetchTable[i].items.size() > 0){ 
        test.scan(queries[i].key,prefetchTable[i].items[0]);
      }
      else{
        test.scan();
      }
    }
    else if (queries[i].type == 0)
    {
      test.get(queries[i].key);
    }
    else if (queries[i].type == 100)
    {
      cout << "No-Operation" << endl;
      continue; 
    }
    else
    {
      cout << "This opcode does not fall within the list available for this DB" << endl;
      cout << "That is why this operation will be ignored and will continue with the rest of the queries" << endl;
    }
  }

  // test.close();

  return 0;
}