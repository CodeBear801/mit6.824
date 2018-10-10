package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func RaftInfo(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		args := append([]interface{}{rf.id, rf.currentTerm, rf.state}, a...)
		log.Printf("[INFO] Raft: [Id: %s | Term: %d | %v] "+format, args...)
	}
	return
}

func RaftDebug(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		args := append([]interface{}{rf.id, rf.currentTerm, rf.state}, a...)
		log.Printf("[DEBUG] Raft: [Id: %s | Term: %d | %v] "+format, args...)
	}
	return
}

func RPCDebug(format string, svcName string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		args := append([]interface{}{svcName}, a...)
		log.Printf("[DEBUG] RPC: [%s] "+format, args...)
	}
	return
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

const RPCTimeout = 50 * time.Millisecond
const RPCMaxTries = 3

// SendRPCRequest tries sending request for multiple times
func SendRPCRequest(requestName string, request func() bool) bool {
	makeRequest := func(successChan chan struct{}) {
		if ok := request(); ok {
			successChan <- struct{}{}
		}
	}

	for attempts := 0; attempts < RPCMaxTries; attempts++ {
		rpcChan := make(chan struct{}, 1)
		go makeRequest(rpcChan)

		select {
		case <-rpcChan:
			return true
		case <-time.After(RPCTimeout):
			fmt.Printf("{rpc time out}\t")
		}
	}
	return false
}
