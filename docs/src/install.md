# Installation

## Download

You can obtain the latest version of Jiffy by cloning the GitHub repository:

```bash
git clone https://github.com/ucbrise/jiffy.git
```

## Configure

To configure the build, Jiffy uses CMake as its build system.Jiffy only 
supports out of source builds; the simplest way to configure the build would be as follows:

```bash
cd jiffy
mkdir -p build
cd build
cmake ..
```

It is possible to configure the build specifying certain options based on 
requirements; the supported options are:

* `BUILD_BENCHMARKS` Build benchmarks (OFF by default)
* `BUILD_TESTS`: Builds all tests (ON by default)
* `BUILD_DOC`: Build documentation (OFF by default)
* `BUILD_PYTHON_CLIENT`: Build Python Client(ON by default)
* `BUILD_JAVA_CLIENT`: Build Java Client(ON by default)

In order to explicitly enable or disable any of these options, set the value of
the corresponding variable to `ON` or `OFF` as follows:

```bash
cmake -DBUILD_TESTS=OFF
```
The external libraries are automatically downloaded and built. User can specify using the existing external libraries installed in system to speed up the building process.The certain options are:

* `USE_SYSTEM_AWS_SDK`: Use system AWS SDK (OFF by default)
* `USE_SYSTEM_BOOST`: Use system boost library (OFF by default)
* `USE_SYSTEM_CATCH`: Use system catch library (OFF by default)
* `USE_SYSTEM_JEMALLOC`: Use system Jemalloc (OFF by default)
* `USE_SYSTEM_LIBCUCKOO`: Use system libcuckoo library (OFF by default)
* `USE_SYSTEM_THRIFT`: Use system thrift library (OFF by default)

Finally, you can configure the install location for Jiffy by modifying the
`CMAKE_INSTALL_PREFIX` variable (which is set to /usr/local by default):

```bash
cmake -DCMAKE_INSTALL_PREFIX=/path/to/installation
```

## Install

Once the build is configured, you can proceed to compile, test and install Jiffy.

For macOS Mojave users, the headers are no longer installed under /usr/include by default. Please run the following command in terminal and follow the installation instructions before building:
```bash
open /Library/Developer/CommandLineTools/Packages/macOS_SDK_headers_for_macOS_10.14.pkg
```

To build, use:

```bash
make
```

or 

```bash
make -j{NUM_CORES}
```

to speed up the build on multi-core systems.

To run the various unit tests, run:

```bash
make test
```

To run the unit tests with all log information, use:

```bash
make test ARGS="-VV"
```

To run the unit tests with log information only when test fails, use:
```bash
make test ARGS="--output-on-failure"
```

and finally, to install, use:

```bash
make install
```
