//go:build unix

package main

import (
	"log"
	"os"
	"syscall"
)

const runtimeUID = 10001

// dropPrivileges lets the container entrypoint fix /data ownership as root and
// then permanently switch to the unprivileged runtime user without setpriv.
func dropPrivileges() {
	if os.Getenv("DROP_PRIVILEGES") != "1" || os.Geteuid() != 0 {
		return
	}
	if err := syscall.Setgroups([]int{runtimeUID}); err != nil {
		log.Fatalf("setgroups: %v", err)
	}
	if err := syscall.Setgid(runtimeUID); err != nil {
		log.Fatalf("setgid: %v", err)
	}
	if err := syscall.Setuid(runtimeUID); err != nil {
		log.Fatalf("setuid: %v", err)
	}
}
