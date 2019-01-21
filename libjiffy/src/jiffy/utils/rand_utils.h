#ifndef JIFFY_RAND_UTILS_H
#define JIFFY_RAND_UTILS_H

#include <cstdint>
#include <random>

namespace jiffy {

namespace utils {
/* Random number utility class */
class rand_utils {
 public:
  /**
   * @brief Generate int64 random number from 0 to max
   * @param max Max number
   * @return Int64 random number
   */

  static int64_t rand_int64(const int64_t &max) {
    return rand_int64(0, max);
  }

  /**
   * @brief Generate int64 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Int64 random number
   */

  static int64_t rand_int64(const int64_t &min, const int64_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<int64_t> distribution(min, max);
    return distribution(generator);
  }

  /**
   * @brief Generate uint64 random number from 0 to max
   * @param max Max number
   * @return Uint64 random number
   */

  static uint64_t rand_uint64(const uint64_t &max) {
    return rand_uint64(0, max);
  }

  /**
   * @brief Generate uint64 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Uint64 random number
   */


  static uint64_t rand_uint64(const uint64_t &min, const uint64_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<uint64_t> distribution(min, max);
    return distribution(generator);
  }

  /**
   * @brief Generate int32 random number from 0 to max
   * @param max Max number
   * @return Int32 random number
   */

  static int32_t rand_int32(const int32_t &max) {
    return rand_int32(0, max);
  }

  /**
   * @brief Generate int32 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Int32 random number
   */

  static int32_t rand_int32(const int32_t &min, const int32_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<int32_t> distribution(min, max);
    return distribution(generator);
  }

  /**
   * @brief Generate uint32 random number from 0 to max
   * @param max Max number
   * @return uint32 random number(in uint64 form)
   */

  static uint64_t rand_uint32(const uint32_t &max) {
    return rand_uint32(0, max);
  }

  /**
   * @brief Generate uint32 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Uint32 random number
   */


  static uint64_t rand_uint32(const uint32_t &min, const uint32_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<uint32_t> distribution(min, max);
    return distribution(generator);
  }
};

}

}

#endif //JIFFY_RAND_UTILS_H
