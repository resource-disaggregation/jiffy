package mmux

import "C"

// #include <mmux/storage/client/kv_listener_wrapper.h>
import "C"
import (
	"unsafe"
	"errors"
)

type KVListener struct {
	client C.kv_listener_ptr
}

type Notification struct {
	op  string
	arg string
}

func convertArray(x[] string) **C.char {
	var o = (**C.char)(C.malloc(C.size_t(len(x)) * C.size_t(unsafe.Sizeof(uintptr(0)))))
	for i := 0; i < len(x); i++ {
		var y = (**C.char) (unsafe.Pointer(uintptr(unsafe.Pointer(o)) + unsafe.Sizeof(*o) * uintptr(i)))
		*y = C.CString(x[i])
	}
	return o
}

func (c KVListener) Free() error {
	if -1 == int(C.destroy_listener(unsafe.Pointer(c.client))) {
		return errors.New("could not destroy kv client")
	}
	return nil
}

func (c KVListener) Subscribe(ops []string) error {
	if -1 == int(C.kv_subscribe(unsafe.Pointer(c.client), convertArray(ops), C.size_t(len(ops)))) {
		return errors.New("could not subscribe")
	}
	return nil
}

func (c KVListener) Unsubscribe(ops []string) error {
	if -1 == int(C.kv_unsubscribe(unsafe.Pointer(c.client), convertArray(ops), C.size_t(len(ops)))) {
		return errors.New("could not unsubscribe")
	}
	return nil
}

func (c KVListener) GetNotification(timeoutMs int64) (Notification, error) {
	var n C.struct_notification_t
	if -1 == C.kv_get_notification(unsafe.Pointer(c.client), C.int64_t(timeoutMs), &n) {
		return Notification{}, errors.New("could not get notification")
	}
	var r Notification
	r.op = C.GoString(n.op)
	r.arg = C.GoString(n.arg)
	return r, nil
}
