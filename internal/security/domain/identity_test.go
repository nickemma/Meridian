package domain

import "testing"

func TestRuleOwnsOnlyCanonicalTenantKeys(t *testing.T) {
	rule := Rule{Identity: "tenant-a", Tenant: "a", Read: true}
	for key, want := range map[string]bool{
		"/tenants/a/strong/item":      true,
		"/tenants/b/strong/item":      false,
		"/tenants/ab/strong/item":     false,
		"/tenants/a/../b/strong/item": false,
		"/tenants/a//item":            false,
		"/tenants/a/item\x00":         false,
		"/tenants/a/\\item":           false,
	} {
		if got := rule.OwnsKey([]byte(key)); got != want {
			t.Errorf("OwnsKey(%q) = %t, want %t", key, got, want)
		}
	}
}

func TestRulePermissions(t *testing.T) {
	reader := Rule{Read: true}
	if !reader.Permits(OperationRead) || reader.Permits(OperationWrite) || reader.Permits(OperationReadWrite) {
		t.Fatal("reader permissions are wrong")
	}
	writer := Rule{Read: true, Write: true}
	if !writer.Permits(OperationReadWrite) || writer.Permits(0) {
		t.Fatal("writer permissions are wrong")
	}
}
