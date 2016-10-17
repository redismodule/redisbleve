package main

import (
	"github.com/redismodule/redisbleve"
	"github.com/wenerme/go-rm/rm"
)

func main() {
	// In case someone try to run this
	rm.Run()
}

func init() {
	rm.Mod = redisbleve.CreateModule()
}
