package lamp

import (
	"crypto/sha1"
	"encoding/hex"
)

type Node struct {
	Addr string `json:"addr,omitempty"`
	Time int64  `json:"time,omitempty"`
}

// generateNodeID
func generateNodeID(addr string) string {
	h := sha1.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}
