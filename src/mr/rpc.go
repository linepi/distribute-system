package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "fmt"

var ErrNoMoreTask = fmt.Errorf("NoMoreTask")
var ErrDone = fmt.Errorf("Done")
var debug = 0

func printf(format string, a ...interface{}) {
	if debug == 1 {
		fmt.Printf(format, a...)
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
