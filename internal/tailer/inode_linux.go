package tailer

import (
	"os"
	"syscall"
)

func getInode(info os.FileInfo) uint64 {
	stat := info.Sys().(*syscall.Stat_t)
	return stat.Ino
}