// tunable parameters: size ratio, choosing between tiering and leveling

#include "db.hpp"
#include <iostream>
#include <fstream>
#include <cmath>

using namespace templatedb;
int tablesize = 100;
bool tiering = false;

// tunable parameters: size ratio, choosing between tiering and leveling
void DB::newfiles() // defining construct to assign values to intialize table and create our file system
{
    // std::string L1_0, L1_1;
    // std::string L2_0, L2_1, L2_2, L2_3;

    // // define file names that we need generated
    // L1_0 = "l1SST0"; 
    // L1_1 = "l1SST1";
    // L2_0 = "l2SST0";
    // L2_1 = "l2SST1";
    // L2_2 = "l2SST2";
    // L2_3 = "l2SST3";

    // // fileNames.push_back(L1_0);
    // // fileNames.push_back(L1_1);
    // // fileNames.push_back(L2_0);
    // // fileNames.push_back(L2_1);
    // // fileNames.push_back(L2_2);
    // // fileNames.push_back(L2_3);

    // std::ifstream fid0(L1_0);
    // if (!fid0.is_open()){
    //     std::ofstream levelingFile(L1_0);
    //     levelingFile << "0,0,-1,-1";
    //     levelingFile.close();
    // }

    // std::ifstream fid1(L1_1);
    // if (!fid1.is_open()){
    //     std::ofstream levelingFile(L1_1);
    //     levelingFile << "0,0,-1,-1";
    //     levelingFile.close();
    // }

    // std::ifstream fid2(L2_0);
    // if (!fid2.is_open()){
    //     std::ofstream levelingFile(L2_0);
    //     levelingFile << "0,0,-1,-1";
    //     levelingFile.close();
    // }

    // std::ifstream fid3(L2_1);
    // if (!fid3.is_open()){
    //     std::ofstream levelingFile(L2_1);
    //     levelingFile << "0,0,-1,-1";
    //     levelingFile.close();
    // }

    // std::ifstream fid4(L2_2);
    // if (!fid4.is_open()){
    //     std::ofstream levelingFile(L2_2);
    //     levelingFile << "0,0,-1,-1";
    //     levelingFile.close();
    // }
    // std::ifstream fid5(L2_3);
    // if (!fid5.is_open()){
    //     std::ofstream levelingFile(L2_3);
    //     levelingFile << "0,0,-1,-1";
    //     levelingFile.close();
    // }

    // std::cout<< "hello\n";

    // this->current_file = 0;
    //  Levels level1;
    //  Levels level2;
    //  level1.numFiles = 2;
    //  level1.fileNames.push_back(L1_0);
    //  level1.fileNames.push_back(L1_1);
    //  //level1.fileNames.insert(level1.fileNames.end(),{L1_0, L1_1});
    //  level1.fileSize = 100;
    //  levelfiles.push_back(level1);

    // level2.numFiles = 4;
    // //level2.fileNames.insert(level2.fileNames.end(),{L2_0, L2_1, L2_2, L2_3});
    // level2.fileNames.push_back(L2_0);
    // level2.fileNames.push_back(L2_1);
    // level2.fileNames.push_back(L2_2);
    // level2.fileNames.push_back(L2_3);
    // level2.fileSize = 200;
    // levelfiles.push_back(level2);

    // levelfiles[2];
    // levelfiles[0].numFiles = 2;
    // levelfiles[0].fileNames.insert(levelfiles[0].fileNames.end(),{L1_0, L1_1});
    // levelfiles[0].fileSize = 100;
    // levelfiles[1].numFiles = 4;
    // levelfiles[1].fileNames.insert(levelfiles[1].fileNames.end(),{L2_0, L2_1, L2_2, L2_3});
    // levelfiles[1].fileSize = 200;
}

Value DB::get(int key) // be able to read from files to get the value we need if not in memtable
{
    int count = 0;
    if (table.count(key))
    {
        std::cout << "Value for key " << std::to_string(key) << " was found within memtable if visiblity of " << std::to_string(table[key].visible) << std::endl;
        return (table[key].visible) ? table[key] : Value(false);
    }
    else
    { // this is where we want to start scanning to files
        std::cout << "Value for key " << std::to_string(key) << " was not found within memtable must check memory" << std::endl;

        write_to_file();
        table.clear();

        bool result;
        int count = 0;

        for (int i = 0; i < levelfiles.size(); i++)
        {
            int index = levelfiles[i].numFiles - 1;
            this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
            for (int j = levelfiles[i].numFiles * (levelfiles[i].fileSize / tablesize); j < 0; j--)
            {
                load_data_file(levelfiles[i].fileNames[index]);
                result = table.count(key);

                if (result)
                {
                    std::cout << "Value was found in memory in file " << levelfiles[i].fileNames[index] << std::endl;
                    return (table[key].visible) ? table[key] : Value(false);
                }

                if (count >= levelfiles[i].fileSize / tablesize)
                {
                    count = 0;
                    this->file.close();
                    index -= 1;
                    this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
                }
            }
        }

        std::cout << "Value was not found in memory in file " << std::endl;
        // this->current_file = 1;
        // this->file.open(fileNames[this->current_file], std::ios::in | std::ios::out);
        // for(int i = 0 ; i < 4 ; i++){ // look into level 1
        //     load_data_file(fileNames[this->current_file]) ;
        //     result = table.count(key);

        //     if(result){
        //         std::cout << "Value was found in memory in file " << fileNames[this->current_file] << std::endl;
        //         return (table[key].visible) ? table[key] : Value(false);
        //     }
        //     if(count >= 2){
        //         count = 0;
        //         this->file.close();
        //         this->current_file -= 1;
        //         this->file.open(fileNames[this->current_file], std::ios::in | std::ios::out);
        //     }
        // }

        // this->file.close();
        // this->current_file = 5;
        // this->file.open(fileNames[this->current_file], std::ios::in | std::ios::out);
        // for(int i = 0 ; i < 16 ; i++){ // look into level 2
        //     load_data_file(fileNames[this->current_file]) ;
        //     result = table.count(key);

        //     if(result){
        //         return (table[key].visible) ? table[key] : Value(false);
        //     }
        //     if(count >= 4){
        //         count = 0;
        //         this->file.close();
        //         this ->current_file -= 1;
        //         this->file.open(fileNames[this->current_file], std::ios::in | std::ios::out);
        //     }
        // }
    }

    std::cout << "closing file" << std::endl;
    // this->file.close();
    std::cout << "reassigning current_file file to 0" << std::endl;
    std::cout << "reassigned current_file file to 0" << std::endl;
    return Value(false);
}

void DB::put(int key, Value val) // complete?
{

    if (table.size() < tablesize)
    {
        table.insert({key, val});
    }
    else
    {
        write_to_file();
        table.clear();
        std::cout << "written to file\n";
        table.insert({key, val});
    }
}

std::vector<Value> DB::scan() // be able to read from files to get the value we need if not in memtable
// OH: print in the terminal or a txt file, best way is to the terminal
{
    std::vector<Value> return_vector;
    for (auto pair : table)
    {
        return_vector.push_back(pair.second);
    }

    return return_vector;
}

std::vector<Value> DB::scan(int min_key, int max_key) // be able to read from files to get the value we need if not in memtable
// OH: return how many values you found (in main method), return a vector of all the values
{
    std::vector<Value> return_vector;
    for (auto pair : table)
    {
        if ((pair.first >= min_key) && (pair.first <= max_key))
            return_vector.push_back(pair.second);
    }

    return return_vector;
}

void DB::del(int key)
{

    bool exisit = table.count(key); // check if this value exisists in memtable
    Value delete_key;
    delete_key.items.push_back(0);
    if (exisit)
    {
        std::cout << "key value " << std::to_string(key) << " exists in the memetable" << std::endl;
        // table.erase(key);
        delete_key.visible = false;
        table[key] = delete_key;
    }
    else
    {
        delete_key.visible = false;
        // table.insert({key, delete_key});
        put(key, delete_key);
    }

    std::cout << "key value " << std::to_string(key) << " is exists if the following value is 1: " << std::to_string(table.count(key)) << std::endl;
}

void DB::del(int min_key, int max_key) // complete?
{

    if (max_key > min_key)
    {
        std::cout << "the min key is greater than max key. Enter correct range for deletion";
    }

    int key = min_key;
    while (key <= max_key)
    {
        bool exisit = table.count(key); // check if this value exisists in memetable
        Value delete_key;
        if (exisit)
        {
            delete_key = table[key];
            // delete_key.items.push_back(0);
            delete_key.visible = false;
            table[key] = delete_key;
        }
        else
        {
            delete_key.visible = false;
            // delete_key.items.push_back(0);
            // table.insert({key, delete_key});
            put(key, delete_key);
        }

        key += 1;
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
        if (op.args.size() > 0)
        {
            this->del(op.key, op.args[0]);
        }
        else
            this->del(op.key);
    }

    return results;
}

bool DB::load_data_file(std::string &fname) // correct this to recognize tombstone
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
            while (std::getline(linestream, item, ' '))
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

bool DB::close()
{
    std::cout << "final size of table before closing is" << std::to_string(table.size()) << std::endl;
    write_to_file();
    for (int i = 0; i < levelfiles.size(); i++)
    {
        for (auto file_check : levelfiles[i].fileNames)
        {
            this->file.open(file_check, std::ios::in | std::ios::out);
            if (file.is_open())
            {
                // this->write_to_file();
                file.close();
            }
            // this->status = CLOSED;
        }
    }
    return true;
}

bool DB::write_to_file() // implement teiring
{
    std::cout << "writing to file" << std::endl;
    int levelRatios = 4;
    // determine min max keys from memtable

    write_to_file(1);
    return true;
}

bool DB::write_to_file(int levelCheck)
{
    if (levelfiles.size() < levelCheck)
    {
        Levels lev;
        lev.numFiles = 0;
        lev.fileSize = tablesize * pow(4, levelCheck); // tablesize; // size of memtable since each run will be considered the size of memtable
                                                       //  lev.numFilesCap = pow(4,(levelCheck));
        lev.numFilesCap = 4;
        levelfiles.push_back(lev);
    }

    if (tiering)
    { // LevelCheck 1 correlates with L0 (levelCheck-1==0)
        if (levelfiles[levelCheck - 1].numFiles >= levelfiles[levelCheck - 1].numFilesCap)
        {
            std::cout << "Reached new Level" << std::endl;
            write_to_file(levelCheck + 1);
            if (levelfiles[levelCheck - 1].numFiles == 0)
            {
                // create new sstable for that level
                std::string newfile = "L" + std::to_string(levelCheck) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
                levelfiles[levelCheck - 1].fileNames.insert(levelfiles[levelCheck - 1].fileNames.end(), {newfile});
                // levelfiles[0].fileNames.push_back();
                levelfiles[levelCheck - 1].numFiles += 1;
                // levelfiles[0].fileSize = 100;
                std::ifstream fid0(newfile);
                if (!fid0.is_open())
                {
                    std::ofstream levelingFile(newfile);
                    levelingFile << "0,0,-1,-1";
                    levelingFile.close();
                }
                std::cout << std::to_string(levelfiles[levelCheck - 1].numFiles) << " SST Created\n";
                this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
                std::cout << "creating this file " << newfile << std::endl;
                std::cout << " opened file to write into" << std::endl;
            }
        }
        else
        {
            // create new sstable for that level
            std::string newfile = "L" + std::to_string(levelCheck) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
            levelfiles[levelCheck - 1].fileNames.insert(levelfiles[levelCheck - 1].fileNames.end(), {newfile});
            // levelfiles[0].fileNames.push_back();
            levelfiles[levelCheck - 1].numFiles += 1;
            // levelfiles[0].fileSize = 100;
            std::ifstream fid0(newfile);
            if (!fid0.is_open())
            {
                std::ofstream levelingFile(newfile);
                levelingFile << "0,0,-1,-1";
                levelingFile.close();
            }
            std::cout << std::to_string(levelfiles[levelCheck - 1].numFiles) << " SST Created\n";
            this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
            std::cout << "creating this file " << newfile << std::endl;
            std::cout << " opened file to write into" << std::endl;
        }
        std::cout << "level check is " << std::to_string(levelCheck) << std::endl;
        if (levelCheck == 1)
        {
            std::string header = std::to_string(table.size()) + ',' + std::to_string(table[table.begin()->first].items.size()) + ',' + std::to_string(table.begin()->first) + ',' + std::to_string(table.rbegin()->first) + '\n';
            file.seekg(0, std::ios::beg);
            file << header;
            file.seekg(0, std::ios::end);
            for (auto item : table)
            {
                std::ostringstream line;
                std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                line << item.second.items.back();
                std::string value(line.str());
                file << item.first << ',' << std::to_string(item.second.visible) << ',' << value << '\n';
            }
            this->file.close();
        }
        else
        {
            std::map<int, Value> mainMemBuffer;
            std::cout << "reached else statement" << std::endl;
            for (auto levelFile : levelfiles[levelCheck - 2].fileNames)
            {
                this->file.open(levelFile);
                std::ifstream fid(levelFile);
                if (fid.is_open())
                {
                    int key;
                    int line_num = 0;
                    std::string line;
                    std::getline(fid, line); // First line is rows, dim, minkey, maxkey
                    while (std::getline(fid, line))
                    {
                        line_num++;
                        std::stringstream linestream(line);
                        std::string item;

                        std::getline(linestream, item, ',');

                        key = stoi(item);
                        std::vector<int> items;
                        while (std::getline(linestream, item, ','))
                        {
                            items.push_back(stoi(item));
                        }
                        mainMemBuffer.insert({key, Value(items)});
                    }
                }
                file.close();
            }
            std::string newfile = levelfiles[levelCheck - 1].fileNames[(levelfiles[levelCheck - 1].numFiles) - 1]; // "L" + std::to_string(levelCheck) + "SST" + std::to_string(levelfiles[levelCheck-1].numFiles);
            std::cout << newfile << std::endl;
            this->file.open(newfile);
            std::cout << std::to_string(mainMemBuffer.size()) << std::endl;
            std::string header = std::to_string(mainMemBuffer.size()) + ',' + std::to_string(mainMemBuffer[mainMemBuffer.begin()->first].items.size()) + ',' + std::to_string(mainMemBuffer.begin()->first) + ',' + std::to_string(mainMemBuffer.rbegin()->first) + '\n';
            file.seekg(0, std::ios::beg);
            file << header;
            // levelingFile << header;
            file.seekg(0, std::ios::end);
            for (auto item : mainMemBuffer)
            {
                std::ostringstream line;
                std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                line << item.second.items.back();
                std::string value(line.str());
                file << item.first << ',' << value << '\n';
                // levelingFile <<  item.first << ',' << value << '\n';
            }
            this->file.close();
            // levelingFile.close();
            for (auto levelFile : levelfiles[levelCheck - 2].fileNames)
            {
                std::remove((levelFile).c_str());
            }
            levelfiles[levelCheck - 2].numFiles = 0;
            levelfiles[levelCheck - 2].fileNames.clear();
            mainMemBuffer.clear();
        }

        return true;
    }
    else
    { // THIS IS LEVELING
        int numelements;
        if (levelfiles[levelCheck - 1].numFiles == 0)
        {
            // create file
            std::string newfile = "L" + std::to_string(levelCheck) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
            levelfiles[levelCheck - 1].fileNames.insert(levelfiles[levelCheck - 1].fileNames.end(), {newfile});
            levelfiles[levelCheck - 1].numFiles += 1;
            std::ifstream fid0(newfile);
            if (!fid0.is_open())
            {
                std::ofstream levelingFile(newfile);
                levelingFile << "0,0,-1,-1";
                numelements = 0;
                levelingFile.close();
            }
            std::cout << std::to_string(levelfiles[levelCheck - 1].numFiles) << " SST Created\n";
            this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
            std::cout << "creating this file " << newfile << std::endl;
            std::cout << " opened file to write into" << std::endl;
        }
        else
        { // in this we want to check num elements in file
            // if numelements + tablesize in file is >= levelfile[levelCheck-1].filesize
            // sort, merge, flush (write_to_file(levelcheck + 1);)
            // else 
            // sort, merge
            std::string levelFile = levelfiles[levelCheck - 1].fileNames[0];
            this->file.open(levelFile);
            std::ifstream fid(levelFile);
            // int numelements;
            if (fid.is_open())
            {
                std::string line;
                std::getline(fid, line); // First line is rows, dim, minkey, maxkey
                std::stringstream linestream(line);
                std::string item;
                std::getline(linestream, item, ',');
                numelements = stoi(item);
            }
            file.close();
            // std::cout << "Num Elements+ Table Size " << std::to_string((numelements + table.size())) << std::endl;
            // std::cout << "Filesize Cap " << std::to_string((levelfiles[levelCheck - 1].fileSize)) << std::endl;
            // std::cout << "Greater/equal? " << std::to_string((numelements + table.size()) >= (levelfiles[levelCheck - 1].fileSize)) << std::endl;
            if ((numelements + table.size()) > (levelfiles[levelCheck - 1].fileSize))
            {
                std::cout << "Reached new Level" << std::endl;
                write_to_file(levelCheck + 1); // Recursive erorr due to this returning to l1 and writing even though past cap
                if (levelfiles[levelCheck - 1].numFiles == 0)
                {
                    // create file
                    std::string newfile = "L" + std::to_string(levelCheck) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
                    levelfiles[levelCheck - 1].fileNames.insert(levelfiles[levelCheck - 1].fileNames.end(), {newfile});
                    levelfiles[levelCheck - 1].numFiles += 1;
                    std::ifstream fid0(newfile);
                    if (!fid0.is_open())
                    {
                        std::ofstream levelingFile(newfile);
                        levelingFile << "0,0,-1,-1";
                        numelements = 0;
                        levelingFile.close();
                    }
                    std::cout << std::to_string(levelfiles[levelCheck - 1].numFiles) << " SST Created\n";
                    this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
                    std::cout << "creating this file " << newfile << std::endl;
                    std::cout << " opened file to write into" << std::endl;
                }
            }
        }
        
        if (levelCheck == 1 && numelements == 0)
        {
            std::string header = std::to_string(table.size()) + ',' + std::to_string(table[table.begin()->first].items.size()) + ',' + std::to_string(table.begin()->first) + ',' + std::to_string(table.rbegin()->first) + '\n';
            file.seekg(0, std::ios::beg);
            file << header;
            file.seekg(0, std::ios::end);
            for (auto item : table)
            {
                std::ostringstream line;
                std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                line << item.second.items.back();
                std::string value(line.str());
                file << item.first << ',' << std::to_string(item.second.visible) << ',' << value << '\n';
            }
            this->file.close();
        }
        else if (levelCheck == 1)
        {
            std::map<int, Value> mainMemBuffer;
            std::cout << "reached else level 1" << std::endl;
            for (auto levelFile : levelfiles[levelCheck - 1].fileNames)
            {
                this->file.open(levelFile);
                std::ifstream fid(levelFile);
                if (fid.is_open())
                {
                    int key;
                    int line_num = 0;
                    std::string line;
                    std::getline(fid, line); // First line is rows, dim, minkey, maxkey
                    while (std::getline(fid, line))
                    {
                        line_num++;
                        std::stringstream linestream(line);
                        std::string item;

                        std::getline(linestream, item, ',');

                        key = stoi(item);
                        std::vector<int> items;
                        while (std::getline(linestream, item, ','))
                        {
                            items.push_back(stoi(item));
                        }
                        mainMemBuffer.insert({key, Value(items)});
                    }
                }
                file.close();
            }
            // mainMemBuffer.insert(table.begin(), table.end());
            for (auto item : table)
            {
                item.second.items.insert(item.second.items.begin(), item.second.visible);               
                mainMemBuffer.insert({item.first, item.second.items});
            }
            std::cout << std::to_string(mainMemBuffer.size()) << std::endl;
            std::string header = std::to_string(mainMemBuffer.size()) + ',' + std::to_string(mainMemBuffer[mainMemBuffer.begin()->first].items.size()) + ',' + std::to_string(mainMemBuffer.begin()->first) + ',' + std::to_string(mainMemBuffer.rbegin()->first) + '\n';
            this->file.open(levelfiles[levelCheck - 1].fileNames[0]);
            file.seekg(0, std::ios::beg);
            file << header;
            // levelingFile << header;
            // file.seekg(0, std::ios::end);
            for (auto item : mainMemBuffer)
            {
                std::ostringstream line;
                std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                line << item.second.items.back();
                std::string value(line.str());
                file << item.first << ',' << value << '\n';
                // levelingFile <<  item.first << ',' << value << '\n';
            }
            this->file.close();
            mainMemBuffer.clear();
        }
        else
        {
            std::map<int, Value> mainMemBuffer;
            if(numelements==0){
                // directly take from levelCheck-2
                std::map<int, Value> mainMemBuffer;
                std::cout << "numelm=0 so we are directly copying from level-2" << std::endl;
                for (auto levelFile : levelfiles[levelCheck - 2].fileNames)
                {
                    this->file.open(levelFile);
                    std::ifstream fid(levelFile);
                    if (fid.is_open())
                    {
                        int key;
                        int line_num = 0;
                        std::string line;
                        std::getline(fid, line); // First line is rows, dim, minkey, maxkey
                        while (std::getline(fid, line))
                        {
                            line_num++;
                            std::stringstream linestream(line);
                            std::string item;

                            std::getline(linestream, item, ',');

                            key = stoi(item);
                            std::vector<int> items;
                            while (std::getline(linestream, item, ','))
                            {
                                items.push_back(stoi(item));
                            }
                            mainMemBuffer.insert({key, Value(items)});
                        }
                        std::cout << std::to_string(line_num) << std::endl;
                    }
                    file.close();
                }
                this->file.open(levelfiles[levelCheck-1].fileNames[0]);
                std::cout << std::to_string(mainMemBuffer.size()) << std::endl;
                std::string header = std::to_string(mainMemBuffer.size()) + ',' + std::to_string(mainMemBuffer[mainMemBuffer.begin()->first].items.size()) + ',' + std::to_string(mainMemBuffer.begin()->first) + ',' + std::to_string(mainMemBuffer.rbegin()->first) + '\n';
                file.seekg(0, std::ios::beg);
                file << header;
                // levelingFile << header;
                file.seekg(0, std::ios::end);
                for (auto item : mainMemBuffer)
                {
                    std::ostringstream line;
                    std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                    line << item.second.items.back();
                    std::string value(line.str());
                    file << item.first << ',' << value << '\n';
                    // levelingFile <<  item.first << ',' << value << '\n';
                }
                this->file.close();
                // levelingFile.close();
                for (auto levelFile : levelfiles[levelCheck - 2].fileNames)
                {
                    std::remove((levelFile).c_str());
                }
                levelfiles[levelCheck - 2].numFiles = 0;
                levelfiles[levelCheck - 2].fileNames.clear();
                mainMemBuffer.clear();    
            }
            else{
                // directly take from levelCheck-1 and levelCheck-2
                std::map<int, Value> mainMemBuffer;
                std::cout << "we are directly copying from level-2 and level-1" << std::endl;
                for (auto levelFile : levelfiles[levelCheck - 2].fileNames)
                {
                    this->file.open(levelFile);
                    std::ifstream fid(levelFile);
                    if (fid.is_open())
                    {
                        int key;
                        int line_num = 0;
                        std::string line;
                        std::getline(fid, line); // First line is rows, dim, minkey, maxkey
                        while (std::getline(fid, line))
                        {
                            line_num++;
                            std::stringstream linestream(line);
                            std::string item;

                            std::getline(linestream, item, ',');

                            key = stoi(item);
                            std::vector<int> items;
                            while (std::getline(linestream, item, ','))
                            {
                                items.push_back(stoi(item));
                            }
                            mainMemBuffer.insert({key, Value(items)});
                        }
                        std::cout << std::to_string(line_num) << std::endl;
                    }
                    file.close();
                }
                for (auto levelFile : levelfiles[levelCheck - 1].fileNames)
                {
                    this->file.open(levelFile);
                    std::ifstream fid(levelFile);
                    if (fid.is_open())
                    {
                        int key;
                        int line_num = 0;
                        std::string line;
                        std::getline(fid, line); // First line is rows, dim, minkey, maxkey
                        while (std::getline(fid, line))
                        {
                            line_num++;
                            std::stringstream linestream(line);
                            std::string item;

                            std::getline(linestream, item, ',');

                            key = stoi(item);
                            std::vector<int> items;
                            while (std::getline(linestream, item, ','))
                            {
                                items.push_back(stoi(item));
                            }
                            mainMemBuffer.insert({key, Value(items)});
                        }
                        std::cout << std::to_string(line_num) << std::endl;
                    }
                    file.close();
                }
                this->file.open(levelfiles[levelCheck-1].fileNames[0]);
                std::cout << std::to_string(mainMemBuffer.size()) << std::endl;
                std::string header = std::to_string(mainMemBuffer.size()) + ',' + std::to_string(mainMemBuffer[mainMemBuffer.begin()->first].items.size()) + ',' + std::to_string(mainMemBuffer.begin()->first) + ',' + std::to_string(mainMemBuffer.rbegin()->first) + '\n';
                file.seekg(0, std::ios::beg);
                file << header;
                // levelingFile << header;
                // file.seekg(0, std::ios::end);
                for (auto item : mainMemBuffer)
                {
                    std::ostringstream line;
                    std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                    line << item.second.items.back();
                    std::string value(line.str());
                    file << item.first << ',' << value << '\n';
                    // levelingFile <<  item.first << ',' << value << '\n';
                }
                this->file.close();
                // levelingFile.close();
                for (auto levelFile : levelfiles[levelCheck - 2].fileNames)
                {
                    std::remove((levelFile).c_str());
                }
                levelfiles[levelCheck - 2].numFiles = 0;
                levelfiles[levelCheck - 2].fileNames.clear();
                mainMemBuffer.clear(); 
            }
            // std::cout << "You've gone too far!!!" << std::endl;
            // if(tunable vairable){
            //  leveling
            // }
            // else{
            //   tiering
            // }

            // compaction
            // std::cout << "error" << std::endl;
            return false;
        }
        return true;
    }
}

// void DB::levelingComp(){

// }