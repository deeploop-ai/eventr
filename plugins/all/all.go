package all

import (
	_ "github.com/deeploop-ai/eventr/plugins/codec/json"
	_ "github.com/deeploop-ai/eventr/plugins/codec/raw"
	_ "github.com/deeploop-ai/eventr/plugins/sink/drop"
	_ "github.com/deeploop-ai/eventr/plugins/sink/http"
	_ "github.com/deeploop-ai/eventr/plugins/sink/kafka"
	_ "github.com/deeploop-ai/eventr/plugins/source/cron"
	_ "github.com/deeploop-ai/eventr/plugins/source/httpserver"
	_ "github.com/deeploop-ai/eventr/plugins/source/kafka"
	_ "github.com/deeploop-ai/eventr/plugins/transform/filter"
	_ "github.com/deeploop-ai/eventr/plugins/transform/map"
	_ "github.com/deeploop-ai/eventr/plugins/transform/route"
	_ "github.com/deeploop-ai/eventr/plugins/transform/wasm"
)
