#include "db.hpp"
#include <iostream>
#include <fstream>
#include <cmath>
#include <ctime>

using namespace templatedb;

Value DB::get(int key) // output result in terminal
{
    int count = 0;
    if (table.count(key))
    {
        return (table[key].visible) ? table[key] : Value(false);
    }
    else
    { // this is where we want to start scanning to files
        write_to_file(1);
        table.clear();

        bool result;
        int count = 0;
        // scanning Levels from top to bottom
        for (int i = 0; i < levelfiles.size(); i++)
        {
            int index = levelfiles[i].numFiles - 1;
            file.close();
            // Scanning runs from latest to oldest
            for (index = levelfiles[i].numFiles - 1; index >= 0; index--)
            {
                this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
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

                // we do not want to read file if it is empty or the key does not fall within the range
                if (numElm == 0 || key > maxKey || key < minKey)
                {
                    file.close();
                    continue;
                }
                std::tuple<bool, int> endfile;
                endfile = std::make_tuple(false, first_position);
                // keep reading the file until we find the result or reached end of file
                while (std::get<0>(endfile) == false)
                {
                    endfile = load_data_file(levelfiles[i].fileNames[index], std::get<1>(endfile));
                    result = table.count(key);
                    if (result)
                    {
                        Value answer;
                        answer = (table[key].visible) ? table[key] : Value(false);
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

    this->file.close();
    table.clear();
    return Value(false);
}

void DB::put(int key, Value val)
{

    if (table.size() < tablesize)
    {
        table.insert({key, val});
    }
    else
    {
        write_to_file(1);
        table.clear();
        table.insert({key, val});
    }
}

void DB::scan() // be able to read from files to get the value we need if not in memtable
{
    if (table.size() != 0)
    {
        write_to_file(1);
        table.clear();
    }

    // Creating scan file for output
    std::string scanFile = dirName + "/" + "Scan_" + std::to_string(time(NULL));
    std::ifstream fid0(scanFile);
    if (!fid0.is_open())
    {
        std::ofstream levelingFile(scanFile);
        levelingFile.close();
    }
    fid0.close();
    std::ofstream levelingFile(scanFile);

    // This for loop traverses through the LSM tree from top level to bottom level
    for (int i = 0; i < levelfiles.size(); i++)
    {
        int index = levelfiles[i].numFiles - 1;
        file.close();

        // This for loop traverses through the runs of each level from newest to oldest
        for (index = levelfiles[i].numFiles - 1; index >= 0; index--)
        {
            this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
            std::ifstream fid(levelfiles[i].fileNames[index]);
            std::string line;
            std::getline(fid, line); // First line is rows, col
            std::tuple<bool, int> endfile;
            endfile = std::make_tuple(false, fid.tellg());
            fid.close();
            // keep reading the file until we find the result or reached end of file
            while (std::get<0>(endfile) == false)
            {
                endfile = load_data_file(levelfiles[i].fileNames[index], std::get<1>(endfile));

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

void DB::scan(int min_key, int max_key)
{
    if (table.size() != 0)
    {
        write_to_file(1);
        table.clear();
    }

    // Creates the a range scan file and uses current system time, as well as min and max key, for naming
    std::string scanFile = dirName + "/" + "RangeScan_" + std::to_string(time(NULL)) + "_" + std::to_string(min_key) + "_" + std::to_string(max_key);
    std::ifstream fid0(scanFile);
    if (!fid0.is_open())
    {
        std::ofstream levelingFile(scanFile);
        levelingFile.close();
    }
    fid0.close();
    std::ofstream levelingFile(scanFile);

    // Similar to the scan file, this for loop traverses through the LSM tree from top level to bottom level
    for (int i = 0; i < levelfiles.size(); i++)
    {
        int index = levelfiles[i].numFiles - 1;
        file.close();

        // This for loop traverses through the runs of each level from newest to oldest
        for (index = levelfiles[i].numFiles - 1; index >= 0; index--)
        {
            this->file.open(levelfiles[i].fileNames[index], std::ios::in | std::ios::out);
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

            // we do not want to read file if it is empty or the key does not fall within the range
            if (numElm == 0 || (minKey > max_key) || (maxKey < min_key))
            {
                file.close();
                continue;
            }
            std::tuple<bool, int> endfile;
            endfile = std::make_tuple(false, fid.tellg());
            fid.close();

            // keep reading the file until we find the result or reached end of file
            while (std::get<0>(endfile) == false)
            {
                endfile = load_data_file(levelfiles[i].fileNames[index], std::get<1>(endfile));

                for (auto item : table)
                {
                    if (item.first >= min_key && item.first <= max_key)
                    {
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
    bool exisit = table.count(key); // check if this value exisists in memtable
    Value delete_key;
    for (int i = 0; i < value_dimensions; i++)
    {
        delete_key.items.push_back(0);
    }
    if (exisit)
    {
        // Sets the visibility of the deleted key to false and inserts it into the table
        delete_key.visible = false;
        table[key] = delete_key;
    }
    else
    {
        // Sets the visibility of the deleted key to false and inserts it into the table
        delete_key.visible = false;
        put(key, delete_key);
    }
}

void DB::del(int min_key, int max_key)
{

    if (max_key < min_key)
    {
        std::cout << "the min key is greater than max key. Enter correct range for deletion";
        return; // exits function if the keys are inputted in the wrong order
    }

    int key = min_key;
    Value delete_key;
    for (int i = 0; i < value_dimensions; i++)
    {
        delete_key.items.push_back(0);
    }
    while (key <= max_key)
    {
        bool exisit = table.count(key); // check if this value exisists in memtable
        if (exisit)
        {
            delete_key = table[key];
            delete_key.visible = false;
            table[key] = delete_key;
        }
        else
        {
            delete_key.visible = false;
            put(key, delete_key);
        }

        key += 1;
    }
}

std::tuple<bool, int> DB::load_data_file(std::string &fname, int pos)
{
    std::ifstream fid(fname);
    std::tuple<bool, int> fileINFO;
    if (fid.is_open())
    {
        int key;
        int line_num = 0;
        std::string line;
        int current_pos;
        fid.seekg(pos, std::ios::beg);

        while (std::getline(fid, line) && line_num < tablesize)
        {
            current_pos = fid.tellg();
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
        }
        if (line_num >= tablesize && !fid.eof())
        {
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
    fileINFO = std::make_tuple(true, fid.tellg());
    fid.close();
    return fileINFO;
}

bool DB::close()
{
    if (table.size() > 0)
    {
        write_to_file(1);
    }

    // Contents in file per row:
    // [level number] [numfiles] [filesize] [numFilesCap] [list of file names]
    std::string meta_tree = dirName + "/" + "meta_Data";
    std::remove(meta_tree.c_str());
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
            this->file.open(file_check, std::ios::in | std::ios::out);
            if (file.is_open())
            {
                file.close();
            }
        }
    }
    return true;
}

bool DB::write_to_file(int levelCheck)
{
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
            write_to_file(levelCheck + 1);
            if (levelfiles[levelCheck - 1].numFiles == 0)
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
                this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
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
            this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
        }
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
            std::string newfile = levelfiles[levelCheck - 1].fileNames[(levelfiles[levelCheck - 1].numFiles) - 1];
            this->file.open(newfile);
            std::string header = std::to_string(mainMemBuffer.size()) + ',' + std::to_string(mainMemBuffer[mainMemBuffer.begin()->first].items.size()) + ',' + std::to_string(mainMemBuffer.begin()->first) + ',' + std::to_string(mainMemBuffer.rbegin()->first) + '\n';
            file.seekg(0, std::ios::beg);
            file << header;
            file.seekg(0, std::ios::end);

            for (auto item : mainMemBuffer)
            {
                std::ostringstream line;
                std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                line << item.second.items.back();
                std::string value(line.str());
                file << item.first << ',' << value << '\n';
            }
            this->file.close();

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
            this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
        }
        else
        { // in this we want to check num elements in file
            std::string levelFile = levelfiles[levelCheck - 1].fileNames[0];
            this->file.open(levelFile);
            std::ifstream fid(levelFile);

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

            if ((numelements + table.size()) > (levelfiles[levelCheck - 1].fileSize))
            {
                write_to_file(levelCheck + 1);
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
                    this->file.open(newfile); // the reason we write directly into level 2, bc it was the last file opened
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

            for (auto item : table)
            {
                item.second.items.insert(item.second.items.begin(), item.second.visible);
                mainMemBuffer.insert({item.first, item.second.items});
            }
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
                this->file.open(levelfiles[levelCheck - 1].fileNames[0]);
                std::string header = std::to_string(mainMemBuffer.size()) + ',' + std::to_string(mainMemBuffer[mainMemBuffer.begin()->first].items.size()) + ',' + std::to_string(mainMemBuffer.begin()->first) + ',' + std::to_string(mainMemBuffer.rbegin()->first) + '\n';
                file.seekg(0, std::ios::beg);
                file << header;
                file.seekg(0, std::ios::end);
                for (auto item : mainMemBuffer)
                {
                    std::ostringstream line;
                    std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                    line << item.second.items.back();
                    std::string value(line.str());
                    file << item.first << ',' << value << '\n';
                }
                this->file.close();
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
                this->file.open(levelfiles[levelCheck - 1].fileNames[0]);
                std::string header = std::to_string(mainMemBuffer.size()) + ',' + std::to_string(mainMemBuffer[mainMemBuffer.begin()->first].items.size()) + ',' + std::to_string(mainMemBuffer.begin()->first) + ',' + std::to_string(mainMemBuffer.rbegin()->first) + '\n';
                file.seekg(0, std::ios::beg);
                file << header;
                for (auto item : mainMemBuffer)
                {
                    std::ostringstream line;
                    std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
                    line << item.second.items.back();
                    std::string value(line.str());
                    file << item.first << ',' << value << '\n';
                }
                this->file.close();
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