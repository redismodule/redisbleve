package redisbleve

import (
	"github.com/blevesearch/bleve"
	"github.com/wenerme/go-rm/rm"
	"io/ioutil"
	"os"
	"unsafe"
)

var ModuleType rm.ModuleType

const ModuleName = "bleve-fts"

func init() {
	dataTypes = append(dataTypes, CreateDataType())
	commands = append(commands,
		CreateCommand_FT_CREATE(),
		CreateCommand_FT_INDEX(),
		CreateCommand_FT_DEL(),
		CreateCommand_FT_GET(),
		CreateCommand_FT_COUNT(),
		CreateCommand_FT_QUERY(),
	)
}
func CreateDataType() rm.DataType {
	return rm.DataType{
		Name:   ModuleName,
		EncVer: 1,
		Desc:   "Fulltext search module build on top of bleve",
		// TODO Load and Save
		Free: func(ptr unsafe.Pointer) {
			val := (*rbData)(ptr)
			err := val.Index.Close()
			if err != nil {
				rm.LogError("Free %v failed: %v", val.Name, err)
			} else {
				rm.LogDebug("Free %v", val.Name)
			}
			os.RemoveAll(val.Path)
		},
	}
}
func CreateCommand_FT_CREATE() rm.Command {
	return rm.Command{
		Usage:    "FT.CREATE index-name",
		Desc:     `Create index`,
		Name:     "ft.create",
		Flags:    rm.BuildCommandFlag(rm.CF_FAST, rm.CF_WRITE, rm.CF_DENY_OOM),
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 2 {
				return ctx.WrongArity()
			}
			ctx.AutoMemory()

			key, ok := openKeyRW(ctx, args[1])
			if !ok {
				return rm.ERR
			}
			var val *rbData
			indexName := args[1].String()
			if key.IsEmpty() {
				dir, err := ioutil.TempDir(os.TempDir(), ModuleName+"-"+indexName)
				if err != nil {
					ctx.ReplyWithError("ERR Failed to create tempdir for index")
					ctx.LogWarn("Failed to create tempdir for index %v: %v", indexName, err)
					return rm.ERR
				}

				mapping := bleve.NewIndexMapping()
				idx, err := bleve.New(dir, mapping)
				if err != nil {
					ctx.ReplyWithError("ERR Failed to create index")
					ctx.LogWarn("Failed to create index %v: %v", indexName, err)
					return rm.ERR
				}
				val = &rbData{
					Name:  indexName,
					Index: idx,
					Path:  dir,
				}
				if key.ModuleTypeSetValue(ModuleType, unsafe.Pointer(val)) == rm.ERR {
					ctx.ReplyWithError("ERR Failed to set module type value")
					return rm.ERR
				}
				ctx.LogDebug("Createt index %v -> %v", indexName, dir)
			} else {
				val = (*rbData)(key.ModuleTypeGetValue())
			}
			ctx.ReplyWithOK()
			ctx.ReplicateVerbatim()
			return rm.OK
		},
	}
}
func CreateCommand_FT_INDEX() rm.Command {
	return rm.Command{
		Usage:    "FT.INDEX index-name doc-id doc",
		Desc:     `Index document`,
		Name:     "ft.index",
		Flags:    rm.BuildCommandFlag(rm.CF_FAST, rm.CF_WRITE, rm.CF_DENY_OOM),
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 4 {
				return ctx.WrongArity()
			}
			ctx.AutoMemory()

			val, ok := mustOpenKeyData(ctx, args[1], rm.WRITE|rm.READ)
			if !ok {
				return rm.ERR
			}
			err := val.Index.Index(args[2].String(), args[3].String())
			if err != nil {
				ctx.ReplyWithError("ERR Failed to index doc")
				ctx.LogNotice("Failed to index doc %v: %v", args[1], args[2])
				return rm.ERR
			}
			ctx.ReplyWithOK()
			ctx.ReplicateVerbatim()
			return rm.OK
		},
	}
}
func CreateCommand_FT_DEL() rm.Command {
	return rm.Command{
		Usage:    "FT.DEL index-name doc-id",
		Desc:     `Delete document`,
		Name:     "ft.del",
		Flags:    rm.BuildCommandFlag(rm.CF_FAST, rm.CF_WRITE, rm.CF_DENY_OOM),
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 3 {
				return ctx.WrongArity()
			}
			ctx.AutoMemory()

			val, ok := mustOpenKeyData(ctx, args[1], rm.WRITE|rm.READ)
			if !ok {
				return rm.ERR
			}
			err := val.Index.Delete(args[2].String())
			if err != nil {
				ctx.ReplyWithError("ERR Failed to delete doc")
				ctx.LogNotice("Failed to delete doc %v: %v %v", args[1], args[2], err)
				return rm.ERR
			}
			ctx.ReplyWithLongLong(1)
			ctx.ReplicateVerbatim()
			return rm.OK
		},
	}
}
func CreateCommand_FT_QUERY() rm.Command {
	return rm.Command{
		Usage:    "FT.QUERY index-name query",
		Desc:     `Query document using query string`,
		Name:     "ft.query",
		Flags:    rm.BuildCommandFlag(rm.CF_FAST, rm.CF_DENY_OOM),
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 3 {
				return ctx.WrongArity()
			}
			ctx.AutoMemory()

			val, ok := mustOpenKeyData(ctx, args[1], rm.WRITE|rm.READ)
			if !ok {
				return rm.ERR
			}

			query := bleve.NewQueryStringQuery(args[2].String())
			searchRequest := bleve.NewSearchRequest(query)
			searchResult, _ := val.Index.Search(searchRequest)

			ctx.ReplyWithArray(int64(searchResult.Hits.Len()))
			for _, m := range searchResult.Hits {
				ctx.ReplyWithSimpleString(m.ID)
			}
			return rm.OK
		},
	}
}
func CreateCommand_FT_COUNT() rm.Command {
	return rm.Command{
		Usage:    "FT.COUNT index-name",
		Desc:     `Document count in index`,
		Name:     "ft.count",
		Flags:    rm.BuildCommandFlag(rm.CF_FAST, rm.CF_DENY_OOM),
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 2 {
				return ctx.WrongArity()
			}
			ctx.AutoMemory()

			val, ok := mustOpenKeyData(ctx, args[1], rm.WRITE|rm.READ)
			if !ok {
				return rm.ERR
			}
			c, err := val.Index.DocCount()
			if err != nil {
				ctx.LogNotice("Faield to get count %v: %v", err)
				ctx.ReplyWithError("ERR Faield to get count")
				return rm.ERR
			}
			ctx.ReplyWithLongLong(int64(c))
			return rm.OK
		},
	}
}

func CreateCommand_FT_GET() rm.Command {
	return rm.Command{
		Usage:    "FT.GET index-name doc-id",
		Desc:     `Get document by id if with source is enabled`,
		Name:     "ft.get",
		Flags:    rm.BuildCommandFlag(rm.CF_FAST, rm.CF_DENY_OOM),
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 3 {
				return ctx.WrongArity()
			}
			ctx.AutoMemory()

			key := ctx.OpenKey(args[1], rm.WRITE)
			if !key.IsEmpty() && key.ModuleTypeGetType() != ModuleType {
				ctx.ReplyWithError(rm.ERRORMSG_WRONGTYPE)
				return rm.ERR
			}
			if key.IsEmpty() {
				ctx.ReplyWithNull()
				return rm.OK
			}

			val := (*rbData)(key.ModuleTypeGetValue())
			doc, err := val.Index.GetInternal([]byte(args[2].String()))
			if err != nil {
				ctx.LogNotice("Faield to get doc %v: %v", val.Name, args[2].String())
				ctx.ReplyWithError("ERR Faield to get doc")
				return rm.ERR
			}
			if len(doc) == 0 {
				ctx.ReplyWithNull()
				return rm.OK
			}

			ctx.ReplyWithSimpleString(string(doc))
			return rm.OK
		},
	}
}

func openKeyRW(ctx rm.Ctx, k rm.String) (rm.Key, bool) {
	key := ctx.OpenKey(k, rm.WRITE|rm.READ)
	if !key.IsEmpty() && key.ModuleTypeGetType() != ModuleType {
		ctx.ReplyWithError(rm.ERRORMSG_WRONGTYPE)
		return key, false
	}
	return key, true
}
func mustOpenKeyData(ctx rm.Ctx, k rm.String, mode int) (*rbData, bool) {
	key := ctx.OpenKey(k, mode)
	if !key.IsEmpty() && key.ModuleTypeGetType() != ModuleType {
		ctx.ReplyWithError(rm.ERRORMSG_WRONGTYPE)
		return nil, false
	}
	if key.IsEmpty() {
		ctx.ReplyWithError("ERR Index not exists")
		return nil, false
	}
	return (*rbData)(key.ModuleTypeGetValue()), true
}

// Redis bleve data
type rbData struct {
	Name       string
	Path       string
	Index      bleve.Index
	WithSource bool
}
