#include "db.hpp"
#include <iostream>
#include <fstream> 

using namespace templatedb;

void DB::newfiles() // defining construct to assign values to intialize table and create our file system
{ 
    std::string L1_0, L1_1;
    std::string L2_0, L2_1, L2_2, L2_3;

    // define file names that we need generated 
    L1_0 = "l1SST0";
    L1_1 = "l1SST1";
    L2_0 = "l2SST0";
    L2_1 = "l2SST1";
    L2_2 = "l2SST2";
    L2_3 = "l2SST3";

    std::ifstream fid0(L1_0);
    if (!fid0.is_open()){
        std::ofstream levelingFile(L1_0);
        levelingFile.close();
    }

    std::ifstream fid1(L1_1);
    if (!fid1.is_open()){
        std::ofstream levelingFile(L1_1);
        levelingFile.close();
    }

    std::ifstream fid2(L2_0);
    if (!fid2.is_open()){
        std::ofstream levelingFile(L2_0);
        levelingFile.close();
    }

    std::ifstream fid3(L2_1);
    if (!fid3.is_open()){
        std::ofstream levelingFile(L2_1);
        levelingFile.close();
    }

    std::ifstream fid4(L2_2);
    if (!fid4.is_open()){
        std::ofstream levelingFile(L2_2);
        levelingFile.close();
    }
    
}

Value DB::get(int key)
{
    if (table.count(key))
        return table[key];
    
    return Value(false);
}


void DB::put(int key, Value val)
{
    table[key] = val;
}


std::vector<Value> DB::scan()
{
    std::vector<Value> return_vector;
    for (auto pair: table)
    {
        return_vector.push_back(pair.second);
    }

    return return_vector;
}


std::vector<Value> DB::scan(int min_key, int max_key)
{
    std::vector<Value> return_vector;
    for (auto pair: table)
    {
        if ((pair.first >= min_key) && (pair.first <= max_key))
            return_vector.push_back(pair.second);
    }

    return return_vector;
}


void DB::del(int key)
{
    table.erase(key);
}


void DB::del(int min_key, int max_key)
{
    for (auto it = table.begin(); it != table.end(); ) {
        if ((it->first >= min_key) && (it->first <= max_key)){
            table.erase(it++);
        } else { 
            ++it;
        }
    }
}


size_t DB::size()
{
    return table.size();
}


std::vector<Value> DB::execute_op(Operation op)
{
    std::vector<Value> results;
    if (op.type == GET)
    {
        results.push_back(this->get(op.key));
    }
    else if (op.type == PUT)
    {
        this->put(op.key, Value(op.args));
    }
    else if (op.type == SCAN)
    {
        results = this->scan(op.key, op.args[0]);
    }
    else if (op.type == DELETE)
    {
        if ( op.args.size()>0 ){
            this->del(op.key, op.args[0]);
        }
        else
            this->del(op.key);
    }

    return results;
}


bool DB::load_data_file(std::string & fname)
{
    std::ifstream fid(fname);
    if (fid.is_open())
    {
        int key;
        int line_num = 0;
        std::string line;
        std::getline(fid, line); // First line is rows, col
        while (std::getline(fid, line))
        {
            line_num++;
            std::stringstream linestream(line);
            std::string item;

            std::getline(linestream, item, ' ');
            std::string op_code = item;

            std::getline(linestream, item, ' ');
            key = stoi(item);
            std::vector<int> items;
            while(std::getline(linestream, item, ' '))
            {
                items.push_back(stoi(item));
            }
            this->put(key, Value(items));
        }
    }
    else
    {
        fprintf(stderr, "Unable to read %s\n", fname.c_str());
        return false;
    }

    return true;
}


db_status DB::open(std::string & fname)
{
    this->file.open(fname, std::ios::in | std::ios::out);
    if (file.is_open())
    {
        this->status = OPEN;
        // New file implies empty file
        if (file.peek() == std::ifstream::traits_type::eof())
            return this->status;

        int key;
        std::string line;
        std::getline(file, line); // First line is rows, col
        while (std::getline(file, line))
        {
            std::stringstream linestream(line);
            std::string item;

            std::getline(linestream, item, ',');
            key = stoi(item);
            std::vector<int> items;
            while(std::getline(linestream, item, ','))
            {
                items.push_back(stoi(item));
            }
            this->put(key, Value(items));
            if (value_dimensions == 0)
                value_dimensions = items.size();
        }
    }
    else if (!file) // File does not exist
    {
        this->file.open(fname, std::ios::out);
        this->status = OPEN;
    }
    else
    {
        file.close();
        this->status = ERROR_OPEN;
    }

    return this->status; 
}


bool DB::close()
{
    if (file.is_open())
    {
        this->write_to_file();
        file.close();
    }
    this->status = CLOSED;

    return true;
}


bool DB::write_to_file()
{
    file.clear();
    file.seekg(0, std::ios::beg);

    std::string header = std::to_string(table.size()) + ',' + std::to_string(value_dimensions) + '\n';
    file << header;
    for(auto item: table)
    {
        std::ostringstream line;
        std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
        line << item.second.items.back();
        std::string value(line.str());
        file << item.first << ',' << value << '\n';
    }

    return true;
}
