package schema

import (
	"context"
	"testing"
)

func TestComputeHash_Deterministic(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", TypeOID: 23, TypeName: "int4"},
		{Name: "name", TypeOID: 25, TypeName: "text"},
	}
	h1 := ComputeHash(cols)
	h2 := ComputeHash(cols)
	if h1 != h2 {
		t.Errorf("hash not deterministic: %s != %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Errorf("expected 64-char hex hash, got %d chars", len(h1))
	}
}

func TestComputeHash_DiffersOnChange(t *testing.T) {
	cols1 := []ColumnDef{{Name: "id", TypeOID: 23, TypeName: "int4"}}
	cols2 := []ColumnDef{{Name: "id", TypeOID: 20, TypeName: "int8"}}
	if ComputeHash(cols1) == ComputeHash(cols2) {
		t.Error("different columns should produce different hashes")
	}
}

func TestSubjectName(t *testing.T) {
	tests := []struct {
		schema, table, want string
	}{
		{"public", "orders", "public.orders"},
		{"myschema", "users", "myschema.users"},
		{"", "items", "public.items"},
	}
	for _, tt := range tests {
		got := SubjectName(tt.schema, tt.table)
		if got != tt.want {
			t.Errorf("SubjectName(%q, %q) = %q, want %q", tt.schema, tt.table, got, tt.want)
		}
	}
}

func TestMemoryStore_RegisterAndGet(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	cols := []ColumnDef{
		{Name: "id", TypeOID: 23, TypeName: "int4"},
		{Name: "name", TypeOID: 25, TypeName: "text"},
	}

	// First register creates version 1.
	s1, created, err := store.Register(ctx, "public.orders", cols)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Error("expected created=true for first register")
	}
	if s1.Version != 1 {
		t.Errorf("expected version 1, got %d", s1.Version)
	}

	// Same columns = no new version.
	s2, created, err := store.Register(ctx, "public.orders", cols)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Error("expected created=false for same columns")
	}
	if s2.Version != 1 {
		t.Errorf("expected version 1, got %d", s2.Version)
	}

	// Different columns = new version.
	cols2 := []ColumnDef{
		{Name: "id", TypeOID: 23, TypeName: "int4"},
		{Name: "name", TypeOID: 25, TypeName: "text"},
		{Name: "email", TypeOID: 25, TypeName: "text"},
	}
	s3, created, err := store.Register(ctx, "public.orders", cols2)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Error("expected created=true for new columns")
	}
	if s3.Version != 2 {
		t.Errorf("expected version 2, got %d", s3.Version)
	}

	// Get specific version.
	got, err := store.Get(ctx, "public.orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != 2 {
		t.Errorf("version 1 should have 2 columns, got %d", len(got.Columns))
	}

	// Latest.
	latest, err := store.Latest(ctx, "public.orders")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Version != 2 {
		t.Errorf("latest should be version 2, got %d", latest.Version)
	}
}

func TestMemoryStore_List(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	// Register 3 versions.
	for i := 1; i <= 3; i++ {
		cols := make([]ColumnDef, i) // different lengths = different hashes
		for j := range cols {
			cols[j] = ColumnDef{Name: "col", TypeOID: uint32(j), TypeName: "text"}
		}
		_, _, err := store.Register(ctx, "public.t", cols)
		if err != nil {
			t.Fatal(err)
		}
	}

	schemas, err := store.List(ctx, "public.t")
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 3 {
		t.Fatalf("expected 3 versions, got %d", len(schemas))
	}
	for i, s := range schemas {
		if s.Version != i+1 {
			t.Errorf("expected version %d, got %d", i+1, s.Version)
		}
	}
}

func TestMemoryStore_GetNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_, err := store.Get(ctx, "nonexistent", 1)
	if err == nil {
		t.Error("expected error for nonexistent schema")
	}
}

func TestMemoryStore_LatestNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_, err := store.Latest(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent subject")
	}
}
