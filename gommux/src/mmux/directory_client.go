package mmux

// #include <mmux/directory/client/directory_client_wrapper.h>
import "C"
import (
	"errors"
	"unsafe"
)

type DirectoryClient struct {
	client C.directory_client_ptr
}

type FileStatus struct {
	fileType      int32
	permissions   uint16
	lastWriteTime uint64
}

type DirectoryEntry struct {
	name   string
	status FileStatus
}

type ReplicaChain struct {
	blockNames  []string
	slotBegin   int
	slotEnd     int
	storageMode int
}

type DataStatus struct {
	backingPath string
	chainLength int
	dataBlocks  []ReplicaChain
	flags       int
}

const PINNED = 0x01
const STATIC_PROVISIONED = 0x02
const MAPPED = 0x04

func convertFileStatus(s C.struct_file_status) FileStatus {
	var r FileStatus
	r.fileType = int32(s._type)
	r.permissions = uint16(s.permissions)
	r.lastWriteTime = uint64(s.last_write_time)
	return r
}

func convertReplicaChain(s C.struct_replica_chain) ReplicaChain {
	var r ReplicaChain
	r.storageMode = int(s.storage_mode)
	r.slotBegin = int(s.slot_begin)
	r.slotEnd = int(s.slot_end)
	var x = s.block_names
	for i := 0; i < int(s.chain_length); i++ {
		var blockName = C.GoString(*x)
		r.blockNames = append(r.blockNames, string(blockName))
		x = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(x)) + unsafe.Sizeof(*x)))
	}
	return r
}

func convertDataStatus(s C.struct_data_status) DataStatus {
	var r DataStatus
	r.flags = int(s.flags)
	r.backingPath = C.GoString(s.backing_path)
	var x = s.data_blocks
	for i := 0; i < int(s.num_blocks); i++ {
		r.dataBlocks = append(r.dataBlocks, convertReplicaChain(*x))
		x = (*C.struct_replica_chain)(unsafe.Pointer(uintptr(unsafe.Pointer(x)) + unsafe.Sizeof(*x)))
	}
	return r
}

func (c DirectoryClient) Free() error {
	if -1 == int(C.destroy_fs(unsafe.Pointer(c.client))) {
		return errors.New("could not destroy directory client")
	}
	return nil
}

func (c DirectoryClient) CreateDirectory(path string) error {
	if -1 == int(C.fs_create_directory(unsafe.Pointer(c.client), C.CString(path))) {
		return errors.New("could not create directory " + path)
	}
	return nil
}

func (c DirectoryClient) CreateDirectories(path string) error {
	if -1 == int(C.fs_create_directories(unsafe.Pointer(c.client), C.CString(path))) {
		return errors.New("could not create directories " + path)
	}
	return nil
}

func (c DirectoryClient) Open(path string) (DataStatus, error) {
	var s C.struct_data_status
	if -1 == int(C.fs_open(unsafe.Pointer(c.client), C.CString(path), &s)) {
		return DataStatus{}, errors.New("could not open file " + path)
	}
	return convertDataStatus(s), nil
}

func (c DirectoryClient) Create(path string, backingPath string, numBlocks int, chainLength int, flags int) (DataStatus, error) {
	var s C.struct_data_status
	if -1 == int(C.fs_create(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath), C.size_t(numBlocks), C.size_t(chainLength), C.int(flags), &s)) {
		return DataStatus{}, errors.New("could not create file " + path)
	}
	return convertDataStatus(s), nil
}

func (c DirectoryClient) OpenOrCreate(path string, backingPath string, numBlocks int, chainLength int, flags int) (DataStatus, error) {
	var s C.struct_data_status
	if -1 == int(C.fs_open_or_create(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath), C.size_t(numBlocks), C.size_t(chainLength), C.int(flags), &s)) {
		return DataStatus{}, errors.New("could not open or create file " + path)
	}
	return convertDataStatus(s), nil
}

func (c DirectoryClient) Exists(path string) (bool, error) {
	var r = C.fs_exists(unsafe.Pointer(c.client), C.CString(path))
	if 1 == r {
		return true, nil
	} else if 0 == r {
		return false, nil
	}
	return false, errors.New("could not check if path exists")
}

func (c DirectoryClient) LastWriteTime(path string) (int64, error) {
	var r = int64(C.fs_last_write_time(unsafe.Pointer(c.client), C.CString(path)))
	if -1 == r {
		return -1, errors.New("could not obtain last write time")
	}
	return r, nil
}

func (c DirectoryClient) GetPermissions(path string) (uint16, error) {
	var r = int(C.fs_get_permissions(unsafe.Pointer(c.client), C.CString(path)))
	if -1 == r {
		return 0, errors.New("could not obtain file permissions")
	}
	return uint16(r), nil
}

func (c DirectoryClient) SetPermissions(path string, perms uint16, opts int) error {
	if -1 == int(C.fs_set_permissions(unsafe.Pointer(c.client), C.CString(path), C.ushort(perms), C.int(opts))) {
		return errors.New("could not set file permissions")
	}
	return nil
}

func (c DirectoryClient) Remove(path string) error {
	if -1 == int(C.fs_remove(unsafe.Pointer(c.client), C.CString(path))) {
		return errors.New("could not remove file " + path)
	}
	return nil
}

func (c DirectoryClient) RemoveAll(path string) error {
	if -1 == int(C.fs_remove_all(unsafe.Pointer(c.client), C.CString(path))) {
		return errors.New("could not recursively remove " + path)
	}
	return nil
}

func (c DirectoryClient) Rename(oldPath string, newPath string) error {
	if -1 == int(C.fs_rename(unsafe.Pointer(c.client), C.CString(oldPath), C.CString(newPath))) {
		return errors.New("could not rename path " + oldPath)
	}
	return nil
}

func (c DirectoryClient) Status(path string) (FileStatus, error) {
	var s C.struct_file_status
	if -1 == int(C.fs_status(unsafe.Pointer(c.client), C.CString(path), &s)) {
		return FileStatus{}, errors.New("could not open file " + path)
	}
	return convertFileStatus(s), nil
}

func (c DirectoryClient) DStatus(path string) (DataStatus, error) {
	var s C.struct_data_status
	if -1 == int(C.fs_dstatus(unsafe.Pointer(c.client), C.CString(path), &s)) {
		return DataStatus{}, errors.New("could not open file " + path)
	}
	return convertDataStatus(s), nil
}

func (c DirectoryClient) IsRegularFile(path string) (bool, error) {
	var r = C.fs_is_regular_file(unsafe.Pointer(c.client), C.CString(path))
	if 1 == r {
		return true, nil
	} else if 0 == r {
		return false, nil
	}
	return false, errors.New("could not check if path is regular file")
}

func (c DirectoryClient) IsDirectory(path string) (bool, error) {
	var r = C.fs_is_directory(unsafe.Pointer(c.client), C.CString(path))
	if 1 == r {
		return true, nil
	} else if 0 == r {
		return false, nil
	}
	return false, errors.New("could not check if path is directory")
}

func (c DirectoryClient) Sync(path string, backingPath string) error {
	if -1 == C.fs_sync(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath)) {
		return errors.New("could not sync path " + path + " with " + backingPath)
	}
	return nil
}

func (c DirectoryClient) Dump(path string, backingPath string) error {
	if -1 == C.fs_dump(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath)) {
		return errors.New("could not dump path " + path + " to " + backingPath)
	}
	return nil
}

func (c DirectoryClient) Load(path string, backingPath string) error {
	if -1 == C.fs_load(unsafe.Pointer(c.client), C.CString(path), C.CString(backingPath)) {
		return errors.New("could not load path " + path + " from " + backingPath)
	}
	return nil
}
