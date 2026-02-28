package chain_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/chain"
	"github.com/florinutz/pgcdc/event"
)

func TestEncrypt_RoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef") // 16 bytes = AES-128
	enc, err := chain.NewEncrypt(key)
	if err != nil {
		t.Fatalf("NewEncrypt returned error: %v", err)
	}

	if enc.Name() != "encrypt" {
		t.Errorf("Name() = %q, want %q", enc.Name(), "encrypt")
	}

	original := `{"key":"value","nested":{"a":1,"b":2}}`
	ev := event.Event{
		ID:        "e1",
		Channel:   "test-channel",
		Operation: "INSERT",
		Payload:   json.RawMessage(original),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}

	result, err := enc.Process(context.Background(), ev)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	// Encrypted payload must differ from the original.
	if string(result.Payload) == original {
		t.Error("encrypted payload equals original; expected different bytes")
	}

	// Output must be valid base64.
	decoded, err := base64.StdEncoding.DecodeString(string(result.Payload))
	if err != nil {
		t.Fatalf("payload is not valid base64: %v", err)
	}

	// Decrypt and verify round-trip.
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("create AES cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("create GCM: %v", err)
	}

	nonceSize := gcm.NonceSize()
	if len(decoded) < nonceSize {
		t.Fatalf("ciphertext too short: %d bytes, nonce requires %d", len(decoded), nonceSize)
	}

	nonce, ciphertext := decoded[:nonceSize], decoded[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		t.Fatalf("GCM decrypt: %v", err)
	}

	if string(plaintext) != original {
		t.Errorf("decrypted = %q, want %q", string(plaintext), original)
	}

	// Other event fields must be preserved.
	if result.ID != "e1" {
		t.Errorf("event ID = %q, want %q", result.ID, "e1")
	}
	if result.Channel != "test-channel" {
		t.Errorf("event Channel = %q, want %q", result.Channel, "test-channel")
	}
}

func TestEncrypt_InvalidKeySize(t *testing.T) {
	badKeys := [][]byte{
		nil,
		{},
		[]byte("short"),
		[]byte("15-bytes-key..."),   // 15 bytes
		[]byte("17-bytes-key....."), // 17 bytes
		[]byte("this-key-is-exactly-33-bytes!!!!X"), // 33 bytes
	}

	for _, key := range badKeys {
		_, err := chain.NewEncrypt(key)
		if err == nil {
			t.Errorf("NewEncrypt(%d bytes) returned nil error, want error", len(key))
		}
	}

	// Verify valid sizes work.
	validKeys := [][]byte{
		[]byte("0123456789abcdef"),                 // 16 bytes (AES-128)
		[]byte("0123456789abcdef01234567"),         // 24 bytes (AES-192)
		[]byte("0123456789abcdef0123456789abcdef"), // 32 bytes (AES-256)
	}
	for _, key := range validKeys {
		_, err := chain.NewEncrypt(key)
		if err != nil {
			t.Errorf("NewEncrypt(%d bytes) returned error: %v", len(key), err)
		}
	}
}

func TestEncrypt_EmptyPayload(t *testing.T) {
	key := []byte("0123456789abcdef")
	enc, err := chain.NewEncrypt(key)
	if err != nil {
		t.Fatalf("NewEncrypt returned error: %v", err)
	}

	ev := event.Event{
		ID:        "e1",
		Channel:   "test-channel",
		Operation: "INSERT",
		Payload:   nil,
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}

	result, err := enc.Process(context.Background(), ev)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if result.Payload != nil {
		t.Errorf("expected nil payload for empty input, got %d bytes", len(result.Payload))
	}

	// Also test with zero-length non-nil payload.
	ev.Payload = json.RawMessage{}
	result, err = enc.Process(context.Background(), ev)
	if err != nil {
		t.Fatalf("Process returned error for zero-length payload: %v", err)
	}

	if len(result.Payload) != 0 {
		t.Errorf("expected empty payload for zero-length input, got %d bytes", len(result.Payload))
	}
}
