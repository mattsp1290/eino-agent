package deps

import (
	einoobs "github.com/mattsp1290/eino-obs"
	"github.com/mattsp1290/eino-tools/applypatch"
	"github.com/mattsp1290/eino-tools/fileops"
	"github.com/mattsp1290/eino-tools/glob"
	"github.com/mattsp1290/eino-tools/search"
	"github.com/mattsp1290/eino-tools/shell"
	"github.com/mattsp1290/eino-tools/trackerwrite"
	"github.com/mattsp1290/eino-tools/urlfetch"
	"github.com/mattsp1290/eino-tools/userinteract"

	"github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agui/convert"
	"github.com/mattsp1290/eino-agui/emitter"
	"github.com/mattsp1290/eino-agui/stream"
	aguitools "github.com/mattsp1290/eino-agui/tools"
)

var (
	_ = schema.System

	_ = convert.ToEinoMessages
	_ = emitter.NewEmitter
	_ = stream.StreamTurn
	_ = aguitools.ClientToolInfos

	_ = einoobs.New

	_ = fileops.NewReadTool
	_ = glob.New
	_ = search.New
	_ = applypatch.New
	_ = shell.New
	_ = urlfetch.New
	_ = userinteract.New
	_ = trackerwrite.New
)
