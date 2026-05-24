package http

import "searchsurge/internal/surgecore"

type Handlers struct {
	engine surgecore.Engine
}

func NewHandlers(engine surgecore.Engine) *Handlers {
	return &Handlers{engine: engine}
}