# CS 561: An Implementation of the LSM Tree

## About

TemplateDB was a simple template given to our team to implement our data systems project in CS 561 in the Spring of 2022. It served as guide for us to extend into our current fully functional LSM tree. This readme will guide you in trying to understand our code and build onto it even further. For a more theoretical understanding of LSM trees and how we setup our code read our final course report `CS561__LSM_Tree_Final_Report.pdf.`

## Requirements

You will need the following on your system (or alternatively develop on the
BU CSA machines)

    1. CMake
    2. C++ Compiler
    3. GoogleTest (Auto compiled and fetched with CMake)

## Usage

To compile, first create a build directory.

```bash
mkdir build
cd build
```

Afterwards, build using cmake.

```bash
cmake ..
cmake --build .
```

An example executable should be located in the `build/example` folder. The
benchmark simply takes in two files, a data file and a workload file and
measures the time it takes to execute the workload over the dataset. Use the
`-h` flag to see the proper syntax.

Additionally we have provided some examples of unit test in C++ using gtest.
This source is located in the `tests/basic_test.cpp`, whith the executable
will be located in `build/tests` directory. We highly recommend when building
your system to continue to expand on unit test. If you want to run all test,
you may use the following command while you are in the build directory.

```bash
ctest
```

Both the basic test and persistence test will go through.

## Building Workloads and Datasets

In the `tools` folder we have included two scripts `gen_data.py` and
`gen_workload.py`. They generate random datasets and workloads respectively.
By default they have a maximum range of values that can be randomly
generated, I assume everyone knows some python and can edit the scripts to
increase the range if needed. Generate workloads and data with the following
syntax

```bash
gen_data.py <rows> <dim_per_value> <folder>
gen_workload.py <rows> <dim_per_value> <max_key> <folder>
```

Data is generated with a space separating each item.
First line indicates

```
Number of Keys  Dimensions of each Object
```

Rest of lines follows the format of

```
OPERATOR KEY VALUE
```

While workloads follow the format of

```
OPERATOR KEY ARGS
```

with the first line being the number of total operations.

## Compiling the LSM Tree?? Main File?

In the `templatedb` folder we have included a `Makefile` which compiles all the important files such as `main.cpp`, `operation.cpp`, and `db.cpp`. Run it simply by typing:

```
make
```

To run the compiled (what do we want to call the final file name) executable, type your command in the following syntax:

```
./mainMade <workload/data file> <table size> <tiering/leveling> <sizeRatio>
```

The input of `<workload/data file>` parameter will be whatever you have generated earlier using the `gen_data.py` or `gen_workload.py` command. This file will usually live in the `/data` folder.

The input of `<table size>` parameter can be any integer greater than 0.

The input of the `<tiering/leveling>` parameter must be either 0 for leveling, or 1 for tiering.

The input of the `<sizeRatio>` parameter can be any integer (preferably less than 10 😂).

### An Example

For example if you wanted to create an LSM tree with table size 100, tiering, 4 runs per level and the dataset `../../data/test_350_3.data`. You would run this command:

```
./mainMade ../../data/test_350_3.data 100 1 4
```

# Debugging/Code Paradigm

## Operation

### operation.hpp

In this C++ header file, we define our classes of operations that our database can accept from external workloads. We currently have implemented 5 types of operations: scan, insert, delete, put, and no operation. This file also contains information on all public and private functions/variables users can access.

### operation.cpp

In this file, we parse our operations from the user given workload/data file and load it into a vector to be executed by our main.cpp file. The ops_from_file function parses each line from the workload/data file, seperating the operation, key value, and arguments. For each parsed line, ops_from_file uses an Operation constructor in which we define the operation type using the parsed operation string.

## Main

### main.cpp

This file contains the main function that will utilitze the functions in the operation.cpp and db.cpp such that the LSM tree can be created and/or queried. The file begins with taking the .data file or the .wl file that is generated by the python files under tools directoring in this project. Then is converts the file into a vector of type queries to determine the query asked per row of the file and the key/arguments for the operation. Once the file has been parsed and the vector of queries have been constructed, the function will loop through to call the correct query operation requested. This is assuming the LSM has already existed. If not, then the first task after the queries have been generated is to create and LSM directory according to its compaction policy, table size, dimenstion of date, and size ratio of levels. If the LSM tree already exisits, then it will go to the correct directory and read the meta data file to read the state of the tree, such as the levels in the tree, files per level, max capacticy per level, etc. 

## DB

### db.hpp

In this C++ header file, we define our classes of Levels, Value, and DB (database) that our database will either use for information or make references to for execution of operations. We currently have implemented 5 types of operations: scan, insert, delete, put, and no operation. This file also contains information on all public and private functions/variables user.

### db.cpp

In this file, implementation of the operations of the LSM tree. The The close function of the db.cpp file writes out the new strucutre the LSM after completing the workload with the information mentioned in the section above (main.cpp). The delete, put, get, scan, and range scan functions were implemented as part of the query operations. All functiosn utiltize the load_data or Write_to_file function in some form which related to acessing main memory content.

### Obtaining results

## Scan
# Scan without input

The Scan operation will provide a file where all the results of the scan are provided in a text file called Scan_{UnviersalTime}. Each time a scan is called, it will generate a new file to write its results.

# Range Scan ( min and max key input)

The Scan operation will provide a file where all the results of the scan are provided in a text file called Scan_{UnviersalTime}_{minkey}_{maxkey}. Each time a scan is called, it will generate a new file to write its results. The Range scan reads the head of each file before deciding to scan through it.

## Get

The results of get operation (Q operation in operation.cpp) will be printed in the terminal.

## Delete

The delete operation can be seen within the files because the value next to the key will have a value 0 after it, representing a tombstone telling the system that the key is 'terminated.'

## Contact

If you have any questions please feel free to email our team:

Adit Mehta

Huda Irshad

Amara Nwigwe

Satha Kittirgan
