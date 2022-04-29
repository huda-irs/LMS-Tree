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

  // templatedb::Operation test;
  // cout << test.type;
  cout << "hello\n";
  std::string tiering = stoi(argv[3]) == 1 ? "T" : "L";
  templatedb::DB test; // DB(input args)
  test.tablesize = stoi(argv[2]);
  test.tiering = (bool)stoi(argv[3]);
  test.sizeRatio = stoi(argv[4]);
  // if(stoi(argv[3]) == 0){
  //   //test = templatedb::DB(stoi(argv[1]), (bool)stoi(argv[2]), stoi(argv[3]));
  //   test.sizeRatio = stoi(argv[4]);
  //   cout<< "hello\n";
  // }
  cout << "check files were created\n";

  std::vector<Operation> queries;

  // queries = templatedb::Operation::ops_from_file("../../data/test_401_3.data");
  queries = templatedb::Operation::ops_from_file(argv[1]);
  cout << queries.size() << " is the number of operations we have\n";

  string meta_tree = tiering + "_" + argv[2] + "_" + argv[4] + "_" + std::to_string(queries[0].args.size());
  if (mkdir(meta_tree.c_str(), 0777) != 0)
  {
    // cout << "could not create file" << endl;
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

    // check if meta_data file exists
    // if so create loop
    // create a temp levels constructor to push back into test.levelfils
    // enter data into temp
    // levelfiles.pushback(temp)
  }

  test.dirName = meta_tree;
  Value prefetchTable[queries.size()];

  for (int i = 0; i < queries.size(); i++)
  {
    prefetchTable[i].items = queries[i].args;
  }

  for (int i = 0; i < queries.size(); i++)
  {
    if (queries[i].type == 3)
    {
      test.put(queries[i].key, prefetchTable[i]);
    }
    else if (queries[i].type == 2)
    {
      test.del(queries[i].key);
    }
    else if (queries[i].type == 1)
    {
      test.scan();
    }
    else if (queries[i].type == 0)
    {
      test.get(queries[i].key);
    }
    else if (queries[i].type == 100)
    {
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