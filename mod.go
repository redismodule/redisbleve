package redisbleve

import "github.com/wenerme/go-rm/rm"

var commands []rm.Command
var dataTypes []rm.DataType

func CreateModule() *rm.Module {
	mod := rm.NewMod()
	mod.Name = "redisbleve"
	mod.Version = 1
	mod.SemVer = "1.0.1-BETA"
	mod.Author = "wenerme"
	mod.Website = "https://github.com/redismodule/redisbleve"
	mod.Desc = `Fulltext search module build on top of bleve`
	mod.Commands = commands
	mod.DataTypes = dataTypes
	mod.AfterInit = func(ctx rm.Ctx, args []rm.String) error {
		ModuleType = rm.GetModuleDataType(ModuleName)
		return nil
	}
	return mod
}
