package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"

	products_service ".../http/services/products"

	cfg ".../pkg/config"
	lgr ".../pkg/logger"
)

var (
	Version = "1.0.0"
	cfgPath string
)

func init() {
	flag.StringVar(&cfgPath, "config-path", "...cfg.toml", "path to config file")
}

func main() {
	flag.Parse()

	// Init config
	_cfg, err := cfg.NewConfig(cfgPath)
	if err != nil {
		fmt.Println("cant init config", err)
	}

	// Init logger
	_ = lgr.GetLogger(_cfg)
	lgr.LOG.Info("===", "custom logger is init")

	// Создание маршрутизатора
	r := mux.NewRouter()

	// Определение обработчиков для разных маршрутов
	r.HandleFunc("/product/create", HandlerWithMethod("PRODUCTS", "CREATE"))
}

func HandlerWithMethod(model, method string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lgr.LOG.Info("-->> ", "Checking auth token")

		authToken := r.Header.Get("Auth-Token")
		if cfg.CFG.HttpServer.CliToken != authToken {
			lgr.LOG.Info("_ACCESS_DENIED_ ", "auth token is not valid")
			fmt.Fprintf(w, `{"status":"error","message":"auth token is not valid"}`)
			return
		}

		lgr.LOG.Info("<<-- ", "Checking auth token")

		switch model {
		case "PRODUCTS":
			ProductsHandler(w, r, method)
		default:
			fmt.Fprintf(w, `{"status":"error","message":"undefined method"}`)
		}
	}
}

func ProductsHandler(w http.ResponseWriter, r *http.Request, method string) {
	lgr.LOG.Info("-->> ", "Serving products handler")

	if r.Method != http.MethodPost {
		lgr.LOG.Warn("ERR: ", "method post only")
		fmt.Fprintf(w, `{"status":"error","message":"method post only"}`)
		return
	}

	var (
		code        int32
		description string
	)

	switch method {
	case "CREATE":
		lgr.LOG.Info("=== ", "METHOD CREATE")
		code, description = products_service.CreateProduct(r)
	default:
		code = 999
		description = "Method not allowed"
	}

	if code == 100 {
		fmt.Fprintf(w, `{"status":"success","message":"data saved"}`)
	} else {
		fmt.Fprintf(w, `{"status":"error","message":"%s"}`, description)
	}

	lgr.LOG.Info("<<-- ", "Serving products handler")
}
