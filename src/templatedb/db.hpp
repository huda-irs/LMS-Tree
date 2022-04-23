#ifndef _TEMPLATEDB_DB_H_
#define _TEMPLATEDB_DB_H_

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <map>

#include "operation.hpp"

namespace templatedb
{

typedef enum _status_code
{
    OPEN = 0,
    CLOSED = 1,
    ERROR_OPEN = 100,
} db_status;


class Value
{
public:
    std::vector<int> items;
    bool visible = true;

    Value() {}
    Value(bool _visible) {visible = _visible;}
    Value(std::vector<int> _items) { items = _items;}

    bool operator ==(Value const & other) const
    {
        return (visible == other.visible) && (items == other.items);
    }
};

class Levels
{
 public:
    int numFiles;
    std::vector<std::string> fileNames;
    int fileSize;
    int numFilesCap;
    Levels(){}
};

class DB
{
public:
    db_status status;

    DB() {};
    ~DB() {close();};
    std::vector<Levels> levelfiles ;
    Value get(int key);
    void put(int key, Value val);
    std::vector<Value> scan();
    std::vector<Value> scan(int min_key, int max_key);
    void del(int key);
    void del(int min_key, int max_key);
    size_t size();

    db_status open(std::string & fname);
    bool close();

    std::tuple<bool,int> load_data_file(std::string & fname, int pointer);

    std::vector<Value> execute_op(Operation op);
     
private:
    std::fstream file;
    //std::unordered_map<int, bool, Value> table; // want to modify this to add tombstone
    //std::unordered_map<int,Value> table;
    std::map<int,Value> table;
    size_t value_dimensions = 0;
    
    bool write_to_file();
    bool write_to_file(int levelcheck);
    void levelingComp();
    void tieringComp();
    //std::vector<std::string> fileNames;
    //int current_file;
};

}   // namespace templatedb

#endif /* _TEMPLATEDB_DB_H_ */