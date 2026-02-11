package api

import (
	"encoding/json"
	"net/http"
)

type ddbError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, typ, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ddbError{Type: typ, Message: message})
}
