package auth

import "testing"

func TestParseScopes(t *testing.T) {
	got, err := ParseScopes("enqueue, work,enqueue")
	if err != nil || len(got) != 2 || got[0] != ScopeEnqueue || got[1] != ScopeWork {
		t.Fatalf("got %v, %v", got, err)
	}
	for _, bad := range []string{"", " , ", "root", "admin,superuser"} {
		if _, err := ParseScopes(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestAdminImpliesEveryScope(t *testing.T) {
	admin := Key{Scopes: []Scope{ScopeAdmin}}
	worker := Key{Scopes: []Scope{ScopeWork}}
	if !admin.Allows(ScopeEnqueue) || !admin.Allows(ScopeWork) {
		t.Fatal("admin should allow everything")
	}
	if worker.Allows(ScopeEnqueue) || worker.Allows(ScopeAdmin) || !worker.Allows(ScopeWork) {
		t.Fatal("work key scopes wrong")
	}
}

func TestHashIsNotThePlaintext(t *testing.T) {
	if h := hash("cvk_secret"); h == "cvk_secret" || len(h) != 64 {
		t.Fatalf("hash = %q", h)
	}
}
