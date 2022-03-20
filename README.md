# CS 561: Data Systems Architecture - TemplateDB


## About

TemplateDB is a simple template for you, the student, to use for the systems
implementation project in CS 561. Note that this is a simple template, it is
not extensive, rather it is meant to help guide you on what we expect when
you implement the LSM tree. You can use this as base code or start from
scratch.


## Requirements

You will need the following on your system (or alternatively develop on the
CSA machines)

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

## Contact

If you have any questions please feel free to see Ju Hyoung in office hours, or
email me at jmun@bu.edu.
