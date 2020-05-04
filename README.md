# Jiffy

A virtual memory abstraction for serverless architectures. See [docs](docs) for detailed documentation.

## Installation

Before you can install Jiffy, make sure you have the following prerequisites:

- MacOS X, CentOS, or Ubuntu(16.04 or later)
- C++ compiler that supports C++11 standard (e.g., GCC 5.3 or later)
- CMake 3.9 or later

For Python client, you will additionally require:

- Python 2.7 or later, 3.6 or later
- Python Packages: setuptools

For java client, you will additionally require:

- Java 1.7 or later
- Maven 3.0.4 or later

### Source build

To download and install Jiffy, use the following commands:
```bash
git clone https://github.com/ucbrise/jiffy.git
cd jiffy
mkdir -p build
cd build
cmake ..
make -j && make test && make install
```
For macOS Mojave users, the headers are no longer installed under `/usr/include` by default.
Please run the following command in terminal and follow the installation instructions before building:

```bash
open /Library/Developer/CommandLineTools/Packages/macOS_SDK_headers_for_macOS_10.14.pkg
```

Go [here](docs/src/quick_start.md) for a quick-start guide!
