package lamp

import (
	"crypto/sha1"
	"encoding/hex"
)

type Address struct {
	Sharding int    `json:"sharding,omitempty"`
	Addr     string `json:"addr,omitempty"`
	Weight   int    `json:"weight,omitempty"`
	ReadOnly int    `json:"readonly,omitempty"`
}

type Node struct {
	Addr     string `json:"addr,omitempty"`
	Weight   int    `json:"weight,omitempty"`
	ReadOnly int    `json:"readonly,omitempty"`
	Time     int64  `json:"time,omitempty"`
}

// generateNodeID
func generateNodeID(addr string) string {
	h := sha1.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}
