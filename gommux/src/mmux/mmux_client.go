package mmux

// #include <mmux/client/cmmux_client.h>
import "C"
import (
	"unsafe"
	"errors"
)

type Client struct {
	client C.mmux_client_ptr
}

func New(host string, dirPort int, leasePort int) (Client, error) {
	var ret Client
	ret.client = (C.mmux_client_ptr)(C.create_mmux_client(C.CString(host), C.int(dirPort), C.int(leasePort)))
	if nil == ret.client {
		return ret, errors.New("could not create client")
	}
	return ret, nil
}

func (c Client) Free() error {
	if -1 == C.destroy_mmux_client(unsafe.Pointer(c.client)) {
		return errors.New("could not destroy client")
	}
	return nil
}

func (c Client) BeginScope(path string) error {
	if -1 == C.mmux_begin_scope(unsafe.Pointer(c.client), C.CString(path)) {
		return errors.New("could not begin scope")
	}
	return nil
}

func (c Client) EndScope(path string) error {
	if -1 == C.mmux_end_scope(unsafe.Pointer(c.client), C.CString(path)) {
		return errors.New("could not end scope")
	}
	return nil
}

func (c Client) GetFS() DirectoryClient {
	var d DirectoryClient
	d.client = (C.directory_client_ptr)(C.mmux_get_fs(unsafe.Pointer(c.client)))
	return d
}

func (c Client) Create(path string, backingPath string, numBlocks int, chainLength int, flags int) KVClient {
	var k KVClient
	k.client = (C.kv_client_ptr)(C.mmux_create(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath), C.ulong(numBlocks), C.ulong(chainLength), C.int(flags)))
	return k
}

func (c Client) Open(path string) KVClient {
	var k KVClient
	k.client = (C.kv_client_ptr)(C.mmux_open(unsafe.Pointer(c.client), C.CString(path)))
	return k
}

func (c Client) OpenOrCreate(path string, backingPath string, numBlocks int, chainLength int, flags int) KVClient {
	var k KVClient
	k.client = (C.kv_client_ptr)(C.mmux_open_or_create(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath), C.ulong(numBlocks), C.ulong(chainLength), C.int(flags)))
	return k
}

func (c Client) Listen(path string) KVListener {
	var l KVListener
	l.client = (C.kv_listener_ptr)(C.mmux_listen(unsafe.Pointer(c.client), C.CString(path)))
	return l
}

func (c Client) Close(path string) error {
	if -1 == int(C.mmux_close(unsafe.Pointer(c.client), C.CString(path))) {
		return errors.New("could not close path " + path)
	}
	return nil
}

func (c Client) Remove(path string) error {
	if -1 == int(C.mmux_remove(unsafe.Pointer(c.client), C.CString(path))) {
		return errors.New("could not remove path " + path)
	}
	return nil
}

func (c Client) Sync(path string, backingPath string) error {
	if -1 == int(C.mmux_sync(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath))) {
		return errors.New("could not sync path " + path + " with " + backingPath)
	}
	return nil
}

func (c Client) Dump(path string, backingPath string) error {
	if -1 == int(C.mmux_dump(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath))) {
		return errors.New("could not dump path " + path + " to " + backingPath)
	}
	return nil
}

func (c Client) Load(path string, backingPath string) error {
	if -1 == int(C.mmux_load(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath))) {
		return errors.New("could not load path " + path + " from " + backingPath)
	}
	return nil
}
