#include <vector>
#include <thread>
#include "./zipf_generator.h"
#include <sstream>
#include <algorithm>
#include <cmath>
#include <ctime>
#include <cstdlib>
#include <fstream>
#include <atomic>
#include <chrono>

void keygenerator(std::size_t num_keys, double theta = 0, int num_buckets = 512) {
  zipfgenerator zipf(theta, num_buckets);
  std::vector<uint64_t> zipfresult;
  std::map<uint64_t, uint64_t> bucket_dist;
  std::string fileName = "../benchmark/src/sorted_full.txt";
  std::ifstream in(fileName.c_str());
  std::ofstream out("zipfkeys.txt");
  std::string str;
  if (!in) {
    std::cerr << "Cannot open the File : " << fileName << std::endl;
  }
  for (std::size_t i = 0; i < num_keys; i++) {
    bucket_dist[zipf.next()]++;
  }
  for(uint64_t i = 0; i < 512;i++)
  {
    for(uint64_t j = 0; j < 78125;j++) {
      in >> str;
      if(j < bucket_dist[i]) {
        out << str << std::endl;
      }
    }
  }
  in.close();
  out.close();
}


int main() {
  size_t num_ops = 419430;
  keygenerator(num_ops);
}