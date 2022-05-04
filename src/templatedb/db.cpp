#include "db.hpp"
#include <iostream>
#include <fstream>
#include <cmath>
#include <ctime>

using namespace templatedb;

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

        write_to_file(1);
        table.clear();

        bool result;
        int count = 0;

        for (int i = 0; i < levelfiles.size(); i++)
        {
            int index = levelfiles[i].numFiles - 1;
            file.close();
            for (index = levelfiles[i].numFiles - 1; index >= 0; index--)
            {
                std::cout << "Searching in Level" << std::to_string(i) << " and sstable " << std::to_string(index) << std::endl;
                std::cout << levelfiles[i].fileNames[index] << std::endl;
                this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
                std::cout << "looking to read header now" << std::endl;
                // read header
                std::ifstream fid(levelfiles[i].fileNames[index]);
                std::string line;
                std::getline(fid, line); // First line is rows, col
                int first_position = fid.tellg();
                std::stringstream linestream(line);
                std::string item;

                std::getline(linestream, item, ',');
                int numElm = stoi(item);

                std::getline(linestream, item, ',');
                int dim = stoi(item);

                std::getline(linestream, item, ',');
                int minKey = stoi(item);

                std::getline(linestream, item, ',');
                int maxKey = stoi(item);
                std::cout << std::to_string(maxKey) << std::endl;
                std::cout << std::to_string(minKey) << std::endl;

                // we do not want to read file if it is empty or the key does not fall within the range
                if (numElm == 0 || key > maxKey || key < minKey)
                {
                    std::cout << "Level" << std::to_string(i) << "SSTable" << std::to_string(index) << " does not have the key range we want. On to the next one" << std::endl;
                    file.close();
                    continue;
                }
                std::tuple<bool, int> endfile;
                endfile = std::make_tuple(false, first_position);
                // keep reading the file until we find the result or reached end of file
                while (std::get<0>(endfile) == false)
                {   
                    std::cout << "Endfile is of size: " << std::to_string(std::tuple_size<decltype(endfile)>::value) << std::endl;
                    std::cout << "About to load file" << std::endl;
                    endfile = load_data_file(levelfiles[i].fileNames[index], std::get<1>(endfile));
                    std::cout << "Loaded data from " << levelfiles[i].fileNames[index];
                    result = table.count(key); 
                    std::cout << "Result is " << result << std::endl;
                    if (result)
                    {
                        Value answer;
                        answer = (table[key].visible) ? table[key] : Value(false);
                        std::cout << "Value was found in memory in file " << levelfiles[i].fileNames[index] << std::endl;
                        table.clear();
                        return answer;
                    }
                    table.clear();
                }
                // reached end of file with no luck so we close that file
                file.close();
            }
        }
    }

    std::cout << "closing file" << std::endl;
    this->file.close();
    std::cout << "reassigning current_file file to 0" << std::endl;
    std::cout << "reassigned current_file file to 0" << std::endl;
    table.clear();
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
        write_to_file(1);
        table.clear();
        std::cout << "written to file\n";
        table.insert({key, val});
    }
}

void DB::scan() // be able to read from files to get the value we need if not in memtable
// OH: print in the terminal or a txt file, best way is to the terminal
{
    std::cout << "SCANNING!!!!" << std::endl;
    // std::vector<Value> return_vector;
    // for (auto pair : table)
    // {
    //     return_vector.push_back(pair.second);
    // }
    // return return_vector; 
    if (table.size() != 0){
        write_to_file(1);
        table.clear();
    }
    
    std::string scanFile = dirName + "/" + "Scan_" + std::to_string(time(NULL));
    std::ifstream fid0(scanFile);
    if (!fid0.is_open())
    {
        std::ofstream levelingFile(scanFile);
        levelingFile.close();
    }
     fid0.close();
     std::ofstream levelingFile(scanFile);
    // this->file.open(scanFile);
      for (int i = 0; i < levelfiles.size(); i++)
        {
            int index = levelfiles[i].numFiles - 1;
            file.close();
            for (index = levelfiles[i].numFiles - 1; index >= 0; index--)
            {
                std::cout << "Searching in Level" << std::to_string(i) << " and sstable " << std::to_string(index) << std::endl;
                std::cout << levelfiles[i].fileNames[index] << std::endl;
                this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
                std::cout << "looking to read header now" << std::endl;
                std::ifstream fid(levelfiles[i].fileNames[index]);
                std::string line;
                std::getline(fid, line); // First line is rows, col
                std::tuple<bool, int> endfile;
                endfile = std::make_tuple(false, fid.tellg());
                fid.close();
                // keep reading the file until we find the result or reached end of file
                while (std::get<0>(endfile) == false)
                {
                    std::cout << "Endfile is of size: " << std::to_string(std::tuple_size<decltype(endfile)>::value) << std::endl;
                    std::cout << "About to load file" << std::endl;
                    endfile = load_data_file(levelfiles[i].fileNames[index], std::get<1>(endfile));
                    std::cout << "Loaded data from " << levelfiles[i].fileNames[index];
                    std::cout << "Table has " << table.size() << " elements." << std::endl;
  
                   for (auto item : table)
                    {
                        std::ostringstream line;
                        std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                        line << item.second.items.back();
                        std::string value(line.str());
                        levelingFile << item.first << ',' << std::to_string(item.second.visible) << ',' << value << '\n';
                    }
                    
                    table.clear();
                }
                // reached end of file with no luck so we close that file
                file.close();
                
            }
        } 
    levelingFile.close();
    table.clear();

}

void DB::scan(int min_key, int max_key) // be able to read from files to get the value we need if not in memtable
// OH: return how many values you found (in main method), return a vector of all the values
{
    std::cout << "Reached range scan and min is " << min_key << " and max is " << max_key << std::endl; 
    if (table.size() != 0){
        write_to_file(1);
        table.clear();
    }

    
    std::string scanFile = dirName + "/" + "RangeScan_" + std::to_string(time(NULL));
    std::ifstream fid0(scanFile);
    if (!fid0.is_open())
    {
        std::ofstream levelingFile(scanFile);
        levelingFile.close();
    }
     fid0.close();
     std::ofstream levelingFile(scanFile);
    // this->file.open(scanFile);
      for (int i = 0; i < levelfiles.size(); i++)
        {
            int index = levelfiles[i].numFiles - 1;
            file.close();
            for (index = levelfiles[i].numFiles - 1; index >= 0; index--)
            {
                std::cout << "Searching in Level" << std::to_string(i) << " and sstable " << std::to_string(index) << std::endl;
                std::cout << levelfiles[i].fileNames[index] << std::endl;
                this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
                std::cout << "looking to read header now" << std::endl;
                std::ifstream fid(levelfiles[i].fileNames[index]);
                std::string line;
                std::getline(fid, line); // First line is rows, col
                int first_position = fid.tellg();
                std::stringstream linestream(line);
                std::string item;

                std::getline(linestream, item, ',');
                int numElm = stoi(item);

                std::getline(linestream, item, ',');
                int dim = stoi(item);

                std::getline(linestream, item, ',');
                int minKey = stoi(item);

                std::getline(linestream, item, ',');
                int maxKey = stoi(item);
                std::cout << std::to_string(maxKey) << std::endl;
                std::cout << std::to_string(minKey) << std::endl;

                // we do not want to read file if it is empty or the key does not fall within the range
                if (numElm == 0 || (minKey > max_key) || (maxKey < min_key))
                {
                    std::cout << "Level" << std::to_string(i) << "SSTable" << std::to_string(index) << " does not have the key range we want. On to the next one" << std::endl;
                    file.close();
                    continue;
                }
                std::tuple<bool, int> endfile;
                endfile = std::make_tuple(false, fid.tellg());
                fid.close();
                // keep reading the file until we find the result or reached end of file
                while (std::get<0>(endfile) == false)
                {
                    std::cout << "Endfile is of size: " << std::to_string(std::tuple_size<decltype(endfile)>::value) << std::endl;
                    std::cout << "About to load file" << std::endl;
                    endfile = load_data_file(levelfiles[i].fileNames[index], std::get<1>(endfile));
                    std::cout << "Loaded data from " << levelfiles[i].fileNames[index];
                    std::cout << "Table has " << table.size() << " elements." << std::endl;
  
                   for (auto item : table)
                    {
                        if(item.first>=min_key && item.first<=max_key){
                            std::ostringstream line;
                            std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                            line << item.second.items.back();
                            std::string value(line.str());
                            levelingFile << item.first << ',' << std::to_string(item.second.visible) << ',' << value << '\n';
                        }
                    }
                    
                    table.clear();
                }
                // reached end of file with no luck so we close that file
                file.close();
            }
        } 
    levelingFile.close();
}

void DB::del(int key)
{
    std::cout << "Delete key within db.cpp is: " << key << std::endl;
    bool exisit = table.count(key); // check if this value exisists in memtable
    Value delete_key;
    for(int i = 0 ; i < value_dimensions ; i++){
        delete_key.items.push_back(0);
    }
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
        this->scan(op.key, op.args[0]);
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

std::tuple<bool, int> DB::load_data_file(std::string &fname, int pos)
{
    std::cout << "In loading file" << std::endl;
    std::ifstream fid(fname);
    std::cout << "Passes loading ifstream" << std::endl;
    std::tuple<bool, int> fileINFO;
    if (fid.is_open())
    {
        int key;
        int line_num = 0;
        std::string line;
        int current_pos;
        // if (pos == 0){
        //     std::getline(fid, line); // First line is rows, col
        // }
        fid.seekg(pos, std::ios::beg);
        while (std::getline(fid, line) && line_num < tablesize)
        {   current_pos = fid.tellg();
            line_num++;

            std::stringstream linestream(line);
            std::string item;

            std::getline(linestream, item, ',');
            key = stoi(item);
            std::getline(linestream, item, ',');
            bool vis = (bool)stoi(item);
            std::vector<int> items;
            while (std::getline(linestream, item, ','))
            {
                items.push_back(stoi(item));
            }
            Value temp = Value(items);
            temp.visible = vis;
            this->put(key, temp);
            // std::cout << "we are on line number " << std::to_string(line_num) << std::endl;
        }
        std::cout << "Last key was " << key << std::endl;
        std::cout << "completed reading " << line_num << " elements" << std::endl;
        if (line_num >= tablesize && !fid.eof())
        {
            std::cout << "Leaving load function at position " << std::to_string(fid.tellg()) << std::endl;
            fileINFO = std::make_tuple(false, current_pos);
            fid.close();
            return fileINFO;
        }
    }
    else
    {
        fprintf(stderr, "Unable to read %s\n", fname.c_str());
        fileINFO = std::make_tuple(false, 0);
        return fileINFO;
    }
    std::cout << "Leaving load function at position " << std::to_string(fid.tellg()) << std::endl;
    fileINFO = std::make_tuple(true, fid.tellg());
    fid.close();
    return fileINFO;
}

bool DB::close()
{
    std::cout << "final size of table before closing is" << std::to_string(table.size()) << std::endl;
    if (table.size() > 0)
    {
        write_to_file(1);
    }

    // Contents in file per row:
    // [level number] [numfiles] [filesize] [numFilesCap] [list of file names]
    std::string meta_tree = dirName + "/" + "meta_Data";
    std::ifstream fid0(meta_tree);
    if (!fid0.is_open())
    {
        std::ofstream levelingFile(meta_tree);
        levelingFile.close();
    }
    fid0.close();
    this->file.open(meta_tree);
    for (int i = 0; i < levelfiles.size(); i++)
    {
        file << std::to_string(i) << " " << std::to_string(levelfiles[i].numFiles) << " " << std::to_string(levelfiles[i].fileSize) << " " << std::to_string(levelfiles[i].numFilesCap);
        for (auto file_check : levelfiles[i].fileNames)
        {
            file << " " << file_check;
        }
        file << "\n";
    }
    for (int i = 0; i < levelfiles.size(); i++)
    {
        for (auto file_check : levelfiles[i].fileNames)
        {
            std::cout << "This is file check " << file_check << std::endl;
            this->file.open(file_check, std::ios::in | std::ios::out);
            if (file.is_open())
            { ////////////
                // std::filebuf *pbuf = file.rdbuf();
                // std::size_t size = pbuf->pubseekoff(0, file.end, file.in);
                // pbuf->pubseekpos(0, file.in);
                // char *buffer = new char[size];
                // pbuf->sgetn(buffer, size);

                // std::cout << "header written to file in level 0 is: " << buffer << std::endl;
                // std::cout << "size of content within file is " << std::to_string(size) << std::endl;
                // //////////
                // this->write_to_file();
                file.close();
            }
            // this->status = CLOSED;
        }
    }
    return true;
}

bool DB::write_to_file(int levelCheck)
{
    std::cout << "Level Check is " << levelCheck << std::endl;
    if (levelfiles.size() < levelCheck)
    {
        Levels lev;
        lev.numFiles = 0;
        lev.fileSize = tablesize * pow(sizeRatio, levelCheck - 1); // size of memtable since each run will be considered the size of memtable
        lev.numFilesCap = sizeRatio;
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
                std::string newfile = dirName + "/" + "L" + std::to_string(levelCheck - 1) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
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
            std::string newfile = dirName + "/" + "L" + std::to_string(levelCheck - 1) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
            levelfiles[levelCheck - 1].fileNames.insert(levelfiles[levelCheck - 1].fileNames.end(), {newfile});
            levelfiles[levelCheck - 1].numFiles += 1;
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
            std::cout << "Header for this file is" << header << std::endl;
            file << header;
            std::cout << "The number of elements in file in level 0 and file number " << std::to_string(levelfiles[0].numFiles) << " is " << std::to_string(table.size()) << std::endl;
            file.seekg(0, std::ios::end);
            // ////////////
            // std::filebuf* pbuf = file.rdbuf();
            // std::size_t size = pbuf->pubseekoff(0, file.end,file.in);
            // pbuf->pubseekpos(0,file.in);
            // char* buffer = new char[size];
            // pbuf->sgetn(buffer,size);

            // std::cout << "header written to file in level 0 is: " << buffer <<std::endl;
            // std::cout << "size of content within file is " << std::to_string(size) << std::endl;
            // //////////
            for (auto item : table)
            {
                std::ostringstream line;
                std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                line << item.second.items.back();
                std::string value(line.str());
                file << item.first << ',' << std::to_string(item.second.visible) << ',' << value << '\n';
            }
            ////////////
            // std::filebuf *pbuf = file.rdbuf();
            // std::size_t size = pbuf->pubseekoff(0, file.end, file.in);
            // pbuf->pubseekpos(0, file.in);
            // char *buffer = new char[size];
            // pbuf->sgetn(buffer, size);

            // std::cout << "header written to file in level 0 is: " << buffer << std::endl;
            // std::cout << "size of content within file is " << std::to_string(size) << std::endl;
            //////////

            this->file.close();
        }
        else
        {
            std::map<int, Value> mainMemBuffer;
            std::cout << "reached else statement" << std::endl;
            for (auto levelFile : levelfiles[levelCheck - 2].fileNames)
            {
                std::cout << "Size of mainbuf" << std::to_string(mainMemBuffer.size()) << std::endl;
                std::cout << "File being added to the mainbuf: " << levelFile << std::endl;
                this->file.open(levelFile);
                std::ifstream fid(levelFile);
                if (fid.is_open())
                {
                    int key;
                    int line_num = 0;
                    std::string line;
                    std::getline(fid, line); // First line is rows, dim, minkey, maxkey
                    std::cout << "Header: " << line << std::endl;
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
                    std::cout << "Lines traversed: " << std::to_string(line_num) << std::endl;
                }
                file.close();
            }
            std::string newfile = levelfiles[levelCheck - 1].fileNames[(levelfiles[levelCheck - 1].numFiles) - 1]; // "L" + std::to_string(levelCheck) + "SST" + std::to_string(levelfiles[levelCheck-1].numFiles);
            std::cout << newfile << std::endl;
            this->file.open(newfile);
            std::cout << "Membuffer Size: " << std::to_string(mainMemBuffer.size()) << std::endl;
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
            std::string newfile = dirName + "/" + "L" + std::to_string(levelCheck - 1) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
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
                    std::string newfile = dirName + "/" + "L" + std::to_string(levelCheck - 1) + "SST" + std::to_string(levelfiles[levelCheck - 1].numFiles);
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
            std::cout << "Membuffer Size: " << std::to_string(mainMemBuffer.size()) << std::endl;
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
            if (numelements == 0)
            {
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
                        std::cout << "Line number: " << std::to_string(line_num) << std::endl;
                    }
                    file.close();
                }
                this->file.open(levelfiles[levelCheck - 1].fileNames[0]);
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
            else
            {
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
                        std::cout << "Line Num is" << std::to_string(line_num) << std::endl;
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
                        std::cout << "Line num is" << std::to_string(line_num) << std::endl;
                    }
                    file.close();
                }
                this->file.open(levelfiles[levelCheck - 1].fileNames[0]);
                std::cout << "Membuf Size: " << std::to_string(mainMemBuffer.size()) << std::endl;
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
            return false;
        }
        return true;
    }
}