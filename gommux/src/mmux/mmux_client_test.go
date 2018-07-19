package mmux

import (
	"os"
	"os/exec"
	"time"
	"testing"
	"runtime"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
)

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.Fail()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.Fail()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.Fail()
	}
}

type TestConf struct {
	directoryExec   string
	storageExec     string
	resourcesPrefix string
	storage1Conf    string
	storage2Conf    string
	storage3Conf    string
	storageAutoConf string
	directoryConf   string
}

func initTestConf() TestConf {
	var conf TestConf
	conf.directoryExec = readEnv("DIRECTORY_SERVER_EXEC", "directoryd")
	conf.storageExec = readEnv("STORAGE_SERVER_EXEC", "storaged")
	conf.resourcesPrefix = readEnv("TEST_RESOURCES_PREFIX", ".")
	conf.storage1Conf = conf.resourcesPrefix + "/storage1.conf"
	conf.storage2Conf = conf.resourcesPrefix + "/storage2.conf"
	conf.storage3Conf = conf.resourcesPrefix + "/storage3.conf"
	conf.storageAutoConf = conf.resourcesPrefix + "/storage_auto_scale.conf"
	conf.directoryConf = conf.resourcesPrefix + "/directory.conf"
	return conf
}

var conf = initTestConf()

type TestConstants struct {
	host string

	directoryServicePort int
	directoryLeasePort   int
	directoryBlockPort   int

	storage1ServicePort      int
	storage1ManagementPort   int
	storage1NotificationPort int
	storage1ChainPort        int

	storage2ServicePort      int
	storage2ManagementPort   int
	storage2NotificationPort int
	storage2ChainPort        int

	storage3ServicePort      int
	storage3ManagementPort   int
	storage3NotificationPort int
	storage3ChainPort        int

	leasePeriod time.Duration
}

func initTestConstants() TestConstants {
	var c TestConstants

	c.host = "127.0.0.1"

	c.directoryServicePort = 9090
	c.directoryLeasePort = 9091
	c.directoryBlockPort = 9092

	c.storage1ServicePort = 9093
	c.storage1ManagementPort = 9094
	c.storage1NotificationPort = 9095
	c.storage1ChainPort = 9096

	c.storage2ServicePort = 9097
	c.storage2ManagementPort = 9098
	c.storage2NotificationPort = 9099
	c.storage2ChainPort = 9100

	c.storage3ServicePort = 9101
	c.storage3ManagementPort = 9102
	c.storage3NotificationPort = 9103
	c.storage3ChainPort = 9104

	c.leasePeriod = time.Second

	return c
}

var constants = initTestConstants()

func readEnv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func waitTillServerReady(host string, port int) {
	for !IsServerAlive(host, port) {
		time.Sleep(100 * time.Millisecond)
	}
}

func startProcess(prog string, conf string) *exec.Cmd {
	cmd := exec.Command(prog, "--config", conf)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		fmt.Print(err.Error() + "\n")
		os.Exit(1)
	}
	return cmd
}

type TestCtx struct {
	directoryCmd *exec.Cmd
	storage1Cmd  *exec.Cmd
	storage2Cmd  *exec.Cmd
	storage3Cmd  *exec.Cmd
}

func startServers(chain bool, autoScale bool) TestCtx {
	var ctx TestCtx

	ctx.directoryCmd = startProcess(conf.directoryExec, conf.directoryConf)

	waitTillServerReady(constants.host, constants.directoryServicePort)
	waitTillServerReady(constants.host, constants.directoryLeasePort)
	waitTillServerReady(constants.host, constants.directoryBlockPort)

	storage1Conf := conf.storage1Conf
	if autoScale {
		storage1Conf = conf.storageAutoConf
	}

	ctx.storage1Cmd = startProcess(conf.storageExec, storage1Conf)
	waitTillServerReady(constants.host, constants.storage1ServicePort)
	waitTillServerReady(constants.host, constants.storage1ManagementPort)
	waitTillServerReady(constants.host, constants.storage1NotificationPort)
	waitTillServerReady(constants.host, constants.storage1ChainPort)

	if chain {
		ctx.storage2Cmd = startProcess(conf.storageExec, conf.storage2Conf)
		waitTillServerReady(constants.host, constants.storage2ServicePort)
		waitTillServerReady(constants.host, constants.storage2ManagementPort)
		waitTillServerReady(constants.host, constants.storage2NotificationPort)
		waitTillServerReady(constants.host, constants.storage2ChainPort)

		ctx.storage3Cmd = startProcess(conf.storageExec, conf.storage3Conf)
		waitTillServerReady(constants.host, constants.storage3ServicePort)
		waitTillServerReady(constants.host, constants.storage3ManagementPort)
		waitTillServerReady(constants.host, constants.storage3NotificationPort)
		waitTillServerReady(constants.host, constants.storage3ChainPort)
	} else {
		ctx.storage2Cmd = nil
		ctx.storage3Cmd = nil
	}

	return ctx
}

func stopProcess(cmd *exec.Cmd) {
	if cmd != nil {
		if err := cmd.Process.Kill(); err != nil {
			fmt.Print(err.Error() + "\n")
			os.Exit(1)
		}
		cmd.Process.Wait()
	}
}

func stopServers(ctx TestCtx) {
	stopProcess(ctx.storage1Cmd)
	stopProcess(ctx.storage2Cmd)
	stopProcess(ctx.storage3Cmd)
	stopProcess(ctx.directoryCmd)
}

func kvOps(t *testing.T, kv KVClient) {
	for i := 0; i < 1000; i++ {
		equals(t, "!ok", kv.Put(strconv.Itoa(i), strconv.Itoa(i)))
	}

	for i := 0; i < 1000; i++ {
		equals(t, strconv.Itoa(i), kv.Get(strconv.Itoa(i)))
	}

	for i := 1000; i < 2000; i++ {
		equals(t, "!key_not_found", kv.Get(strconv.Itoa(i)))
	}

	for i := 0; i < 1000; i++ {
		equals(t, strconv.Itoa(i), kv.Update(strconv.Itoa(i), strconv.Itoa(i+1000)))
	}

	for i := 1000; i < 2000; i++ {
		equals(t, "!key_not_found", kv.Update(strconv.Itoa(i), strconv.Itoa(i+1000)))
	}

	for i := 0; i < 1000; i++ {
		equals(t, strconv.Itoa(i+1000), kv.Get(strconv.Itoa(i)))
	}

	for i := 0; i < 1000; i++ {
		equals(t, strconv.Itoa(i+1000), kv.Remove(strconv.Itoa(i)))
	}

	for i := 1000; i < 2000; i++ {
		equals(t, "!key_not_found", kv.Get(strconv.Itoa(i)))
	}

	for i := 0; i < 1000; i++ {
		equals(t, "!key_not_found", kv.Get(strconv.Itoa(i)))
	}
}

func TestCreate(t *testing.T) {

	ctx := startServers(false, false)

	client, err := New(constants.host, constants.directoryServicePort, constants.directoryLeasePort)
	ok(t, err)

	kv := client.Create("/a/file.txt", "local://tmp", 1, 1, 0)
	kvOps(t, kv)

	exists, err := client.GetFS().Exists("/a/file.txt")
	ok(t, err)
	assert(t, exists, "file /a/file.txt does not exist")

	stopServers(ctx)
}

func TestOpen(t *testing.T) {
	ctx := startServers(false, false)

	client, err := New(constants.host, constants.directoryServicePort, constants.directoryLeasePort)
	ok(t, err)

	client.Create("/a/file.txt", "local://tmp", 1, 1, 0)

	exists, err := client.GetFS().Exists("/a/file.txt")
	ok(t, err)
	assert(t, exists, "file /a/file.txt does not exist")

	kv := client.Open("/a/file.txt")
	kvOps(t, kv)

	stopServers(ctx)
}

func TestSyncRemove(t *testing.T) {
	ctx := startServers(false, false)

	client, err := New(constants.host, constants.directoryServicePort, constants.directoryLeasePort)
	ok(t, err)

	client.Create("/a/file.txt", "local://tmp", 1, 1, 0)

	exists, err := client.GetFS().Exists("/a/file.txt")
	ok(t, err)
	assert(t, exists, "file /a/file.txt does not exist")

	err = client.Sync("/a/file.txt", "local://tmp")
	ok(t, err)
	exists, err = client.GetFS().Exists("/a/file.txt")
	ok(t, err)
	assert(t, exists, "file /a/file.txt does not exist")

	err = client.Remove("/a/file.txt")
	ok(t, err)
	exists, err = client.GetFS().Exists("/a/file.txt")
	ok(t, err)
	assert(t, !exists, "file /a/file.txt still exists")

	stopServers(ctx)
}

func TestClose(t *testing.T) {
	ctx := startServers(false, false)

	client, err := New(constants.host, constants.directoryServicePort, constants.directoryLeasePort)
	ok(t, err)

	client.Create("/a/file1.txt", "local://tmp", 1, 1, 0)
	client.Create("/a/file2.txt", "local://tmp", 1, 1, PINNED)
	client.Create("/a/file3.txt", "local://tmp", 1, 1, MAPPED)

	exists, err := client.GetFS().Exists("/a/file1.txt")
	ok(t, err)
	assert(t, exists, "file /a/file1.txt does not exist")
	exists, err = client.GetFS().Exists("/a/file2.txt")
	ok(t, err)
	assert(t, exists, "file /a/file2.txt does not exist")
	exists, err = client.GetFS().Exists("/a/file3.txt")
	ok(t, err)
	assert(t, exists, "file /a/file3.txt does not exist")

	err = client.Close("/a/file1.txt")
	ok(t, err)
	err = client.Close("/a/file2.txt")
	ok(t, err)
	err = client.Close("/a/file3.txt")
	ok(t, err)

	time.Sleep(constants.leasePeriod)

	exists, err = client.GetFS().Exists("/a/file1.txt")
	ok(t, err)
	assert(t, exists, "file /a/file1.txt does not exist")
	exists, err = client.GetFS().Exists("/a/file2.txt")
	ok(t, err)
	assert(t, exists, "file /a/file2.txt does not exist")
	exists, err = client.GetFS().Exists("/a/file3.txt")
	ok(t, err)
	assert(t, exists, "file /a/file3.txt does not exist")

	time.Sleep(2 * constants.leasePeriod)

	exists, err = client.GetFS().Exists("/a/file1.txt")
	ok(t, err)
	assert(t, !exists, "file /a/file1.txt still exists")
	exists, err = client.GetFS().Exists("/a/file2.txt")
	ok(t, err)
	assert(t, exists, "file /a/file2.txt does not exist")
	exists, err = client.GetFS().Exists("/a/file3.txt")
	ok(t, err)
	assert(t, exists, "file /a/file3.txt does not exist")

	stopServers(ctx)
}

func TestAutoScaling(t *testing.T) {
	ctx := startServers(false, true)

	client, err := New(constants.host, constants.directoryServicePort, constants.directoryLeasePort)
	ok(t, err)

	kv := client.Create("/a/file.txt", "local://tmp", 1, 1, 0)
	for i := 0; i < 2000; i++ {
		equals(t, "!ok", kv.Put(strconv.Itoa(i), strconv.Itoa(i)))
	}
	s, err := kv.GetStatus()
	ok(t, err)
	equals(t, 4, len(s.dataBlocks))

	for i := 0; i < 2000; i++ {
		equals(t, strconv.Itoa(i), kv.Remove(strconv.Itoa(i)))
	}
	s, err = kv.GetStatus()
	ok(t, err)
	equals(t, 1, len(s.dataBlocks))

	stopServers(ctx)
}

func TestChainReplication(t *testing.T) {
	ctx := startServers(true, false)

	client, err := New(constants.host, constants.directoryServicePort, constants.directoryLeasePort)
	ok(t, err)

	kv := client.Create("/a/file.txt", "local://tmp", 1, 3, 0)
	kvOps(t, kv)

	stopServers(ctx)
}

func TestNotifications(t *testing.T) {
	ctx := startServers(false, false)

	client, err := New(constants.host, constants.directoryServicePort, constants.directoryLeasePort)
	ok(t, err)

	kv := client.Create("/a/file.txt", "local://tmp", 1, 1, 0)
	n1 := client.Listen("/a/file.txt")
	n2 := client.Listen("/a/file.txt")
	n3 := client.Listen("/a/file.txt")

	err = n1.Subscribe([]string{"put"})
	ok(t, err)
	err = n2.Subscribe([]string{"put", "remove"})
	ok(t, err)
	err = n3.Subscribe([]string{"remove"})
	ok(t, err)

	kv.Put("key1", "value1")
	kv.Remove("key1")

	N, err := n1.GetNotification(-1)
	ok(t, err)
	equals(t, Notification{"put", "key1"}, N)

	N, err = n2.GetNotification(-1)
	ok(t, err)
	equals(t, Notification{"put", "key1"}, N)

	N, err = n2.GetNotification(-1)
	ok(t, err)
	equals(t, Notification{"remove", "key1"}, N)

	N, err = n3.GetNotification(-1)
	ok(t, err)
	equals(t, Notification{"remove", "key1"}, N)

	err = n1.Unsubscribe([]string{"put"})
	ok(t, err)
	err = n2.Unsubscribe([]string{"remove"})
	ok(t, err)

	kv.Put("key1", "value1")
	kv.Remove("key1")

	N, err = n2.GetNotification(-1)
	ok(t, err)
	equals(t, Notification{"put", "key1"}, N)

	N, err = n3.GetNotification(-1)
	ok(t, err)
	equals(t, Notification{"remove", "key1"}, N)

	stopServers(ctx)
}