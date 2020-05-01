#ifndef JIFFY_MAESTRO_OPS_EXCEPTION_H
#define JIFFY_MAESTRO_OPS_EXCEPTION_H

#include <exception>
#include <string>

namespace jiffy {
  namespace maestro {
    class maestro_ops_exception : public std::exception {

    public:
      /*
       * Constructor
       */
      explicit maestro_ops_exception(std::string msg);

      /*
       * Fetch exception message
       */

      char const *what() const noexcept override;

    private:
      /* Exception message */
      std::string msg_;

    };
  }
}


#endif //JIFFY_MAESTRO_OPS_EXCEPTION_H
