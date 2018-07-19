package mmux

import "C"

// #include <mmux/utils/connection_utils.h>
// #include <mmux/utils/logging.h>
import "C"
import "errors"

func IsServerAlive(host string, port int) bool {
	return int(C.is_server_alive(C.CString(host), C.int(port))) == 1
}

func ConfigureLogging(logLevel int) error {
	if -1 == C.configure_logging(C.int(logLevel)) {
		return errors.New("")
	}
	return nil
}
