package client

import (
	"net/http"
)

func HTTPClient() *http.Client {
	httpClient := &http.Client{}
	return httpClient
}
