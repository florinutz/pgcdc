package chain

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/florinutz/pgcdc/event"
)

// Encrypt implements the Link interface. It encrypts the event payload using AES-GCM.
type Encrypt struct {
	key []byte
}

// NewEncrypt creates a new encrypt link. The key must be 16, 24, or 32 bytes for AES-128/192/256.
func NewEncrypt(key []byte) (*Encrypt, error) {
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("invalid AES key size %d: must be 16, 24, or 32 bytes", len(key))
	}
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return &Encrypt{key: keyCopy}, nil
}

func (e *Encrypt) Name() string { return "encrypt" }

// Process encrypts ev.Payload using AES-GCM, replacing the payload with the ciphertext (base64-encoded).
func (e *Encrypt) Process(_ context.Context, ev event.Event) (event.Event, error) {
	if len(ev.Payload) == 0 {
		return ev, nil
	}

	ev.EnsurePayload()

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return ev, fmt.Errorf("create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return ev, fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return ev, fmt.Errorf("generate nonce: %w", err)
	}

	// Seal appends the ciphertext (with auth tag) after the nonce.
	ciphertext := gcm.Seal(nonce, nonce, ev.Payload, nil)

	encoded := base64.StdEncoding.EncodeToString(ciphertext)
	ev.Payload = []byte(encoded)
	return ev, nil
}
