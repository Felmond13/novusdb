package storage

import (
	"testing"
)

func TestDocumentSetGet(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", "test")
	doc.Set("age", int64(30))
	doc.Set("active", true)
	doc.Set("score", 3.14)

	v, ok := doc.Get("name")
	if !ok || v != "test" {
		t.Errorf("expected name=test, got %v", v)
	}
	v, ok = doc.Get("age")
	if !ok || v != int64(30) {
		t.Errorf("expected age=30, got %v", v)
	}
	v, ok = doc.Get("active")
	if !ok || v != true {
		t.Errorf("expected active=true, got %v", v)
	}
	v, ok = doc.Get("score")
	if !ok || v != 3.14 {
		t.Errorf("expected score=3.14, got %v", v)
	}
}

func TestDocumentNested(t *testing.T) {
	doc := NewDocument()
	doc.SetNested([]string{"params", "timeout"}, int64(60))
	doc.SetNested([]string{"params", "retry"}, int64(3))

	v, ok := doc.GetNested([]string{"params", "timeout"})
	if !ok || v != int64(60) {
		t.Errorf("expected params.timeout=60, got %v", v)
	}
	v, ok = doc.GetNested([]string{"params", "retry"})
	if !ok || v != int64(3) {
		t.Errorf("expected params.retry=3, got %v", v)
	}
}

func TestDocumentEncodeDecode(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", "workflow1")
	doc.Set("retry", int64(5))
	doc.Set("enabled", true)
	doc.Set("rate", 0.75)

	// Document imbriqué
	sub := NewDocument()
	sub.Set("timeout", int64(30))
	doc.Set("params", sub)

	encoded, err := doc.Encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Vérifier chaque champ
	v, ok := decoded.Get("name")
	if !ok || v != "workflow1" {
		t.Errorf("expected name=workflow1, got %v", v)
	}
	v, ok = decoded.Get("retry")
	if !ok || v != int64(5) {
		t.Errorf("expected retry=5, got %v", v)
	}
	v, ok = decoded.Get("enabled")
	if !ok || v != true {
		t.Errorf("expected enabled=true, got %v", v)
	}
	v, ok = decoded.Get("rate")
	if !ok || v != 0.75 {
		t.Errorf("expected rate=0.75, got %v", v)
	}

	// Sous-document
	subVal, ok := decoded.Get("params")
	if !ok {
		t.Fatal("expected params field")
	}
	subDoc, ok := subVal.(*Document)
	if !ok {
		t.Fatal("expected params to be a Document")
	}
	timeout, ok := subDoc.Get("timeout")
	if !ok || timeout != int64(30) {
		t.Errorf("expected params.timeout=30, got %v", timeout)
	}
}

func TestDocumentNull(t *testing.T) {
	doc := NewDocument()
	doc.Set("empty", nil)

	encoded, err := doc.Encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	v, ok := decoded.Get("empty")
	if !ok {
		t.Fatal("expected empty field to exist")
	}
	if v != nil {
		t.Errorf("expected empty=nil, got %v", v)
	}
}

func TestDocumentUpdate(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", "original")
	doc.Set("name", "updated")

	v, ok := doc.Get("name")
	if !ok || v != "updated" {
		t.Errorf("expected name=updated, got %v", v)
	}
	if len(doc.Fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(doc.Fields))
	}
}
