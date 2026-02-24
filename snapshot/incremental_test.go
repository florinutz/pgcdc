package snapshot

import (
	"testing"
)

func TestBuildChunkQuery_SinglePK_FirstChunk(t *testing.T) {
	query, args := buildChunkQuery(`"orders"`, []string{"id"}, nil, 500)

	wantQuery := `SELECT * FROM "orders" ORDER BY "id" LIMIT 500`
	if query != wantQuery {
		t.Errorf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 0 {
		t.Errorf("args = %v, want empty", args)
	}
}

func TestBuildChunkQuery_SinglePK_Subsequent(t *testing.T) {
	query, args := buildChunkQuery(`"orders"`, []string{"id"}, []any{42}, 1000)

	wantQuery := `SELECT * FROM "orders" WHERE ("id") > ($1) ORDER BY "id" LIMIT 1000`
	if query != wantQuery {
		t.Errorf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 1 || args[0] != 42 {
		t.Errorf("args = %v, want [42]", args)
	}
}

func TestBuildChunkQuery_CompositePK_FirstChunk(t *testing.T) {
	query, args := buildChunkQuery(`"order_items"`, []string{"order_id", "item_id"}, nil, 200)

	wantQuery := `SELECT * FROM "order_items" ORDER BY "order_id", "item_id" LIMIT 200`
	if query != wantQuery {
		t.Errorf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 0 {
		t.Errorf("args = %v, want empty", args)
	}
}

func TestBuildChunkQuery_CompositePK_Subsequent(t *testing.T) {
	query, args := buildChunkQuery(`"order_items"`, []string{"order_id", "item_id"}, []any{10, 3}, 200)

	wantQuery := `SELECT * FROM "order_items" WHERE ("order_id", "item_id") > ($1, $2) ORDER BY "order_id", "item_id" LIMIT 200`
	if query != wantQuery {
		t.Errorf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 2 {
		t.Fatalf("args len = %d, want 2", len(args))
	}
	if args[0] != 10 || args[1] != 3 {
		t.Errorf("args = %v, want [10, 3]", args)
	}
}

func TestBuildChunkQuery_EmptyAfterPK(t *testing.T) {
	query, args := buildChunkQuery(`"users"`, []string{"id"}, []any{}, 100)

	wantQuery := `SELECT * FROM "users" ORDER BY "id" LIMIT 100`
	if query != wantQuery {
		t.Errorf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 0 {
		t.Errorf("args = %v, want empty", args)
	}
}
