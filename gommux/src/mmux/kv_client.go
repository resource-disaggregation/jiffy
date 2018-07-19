package mmux

// #include <mmux/storage/client/kv_client_wrapper.h>
import "C"
import (
	"unsafe"
	"errors"
)

type KVClient struct {
	client C.kv_client_ptr
}

func (c KVClient) Free() error {
	if -1 == int(C.destroy_kv(unsafe.Pointer(c.client))) {
		return errors.New("could not destroy kv client")
	}
	return nil
}

func (c KVClient) Refresh() error {
	if -1 == int(C.kv_refresh(unsafe.Pointer(c.client))) {
		return errors.New("could not refresh client")
	}
	return nil
}

func (c KVClient) GetStatus() (DataStatus, error) {
	var s C.struct_data_status
	if -1 == C.kv_get_status(unsafe.Pointer(c.client), &s) {
		return DataStatus{}, errors.New("could not get status")
	}
	return convertDataStatus(s), nil
}

func (c KVClient) Put(key string, value string) string {
	return C.GoString(C.kv_put(unsafe.Pointer(c.client), C.CString(key), C.CString(value)))
}

func (c KVClient) Get(key string) string {
	return C.GoString(C.kv_get(unsafe.Pointer(c.client), C.CString(key)))
}

func (c KVClient) Update(key string, value string) string {
	return C.GoString(C.kv_update(unsafe.Pointer(c.client), C.CString(key), C.CString(value)))
}

func (c KVClient) Remove(key string) string {
	return C.GoString(C.kv_remove(unsafe.Pointer(c.client), C.CString(key)))
}
