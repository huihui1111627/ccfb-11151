package lake

import (
	"crypto/sha256"
	"encoding/hex"
)

// hasher is a tiny streaming sha256 wrapper for deterministic ids.
type hasher struct{ h [32]byte }

func newHasher() *hasher { return &hasher{} }
func (x *hasher) writeString(s string) {
	sum := sha256.Sum256(append(x.h[:], []byte(s)...))
	x.h = sum
}
func (x *hasher) hex16() string { return hex.EncodeToString(x.h[:])[:16] }
