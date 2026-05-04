package driver

import "testing"

func TestParseDSNUsesEnvPasswordWhenDSNHasNoPassword(t *testing.T) {
	path, password := parseDSNWithEnv("db.sdf", func(key string) string {
		if key != passwordEnvVar {
			t.Fatalf("unexpected env lookup %q", key)
		}
		return "from-env"
	})

	if path != "db.sdf" {
		t.Fatalf("path = %q, want db.sdf", path)
	}
	if password != "from-env" {
		t.Fatalf("password = %q, want env password", password)
	}
}

func TestParseDSNPasswordParameterTakesPrecedenceOverEnv(t *testing.T) {
	path, password := parseDSNWithEnv("db.sdf?password=from-dsn&mode=ro", func(string) string {
		return "from-env"
	})

	if path != "db.sdf" {
		t.Fatalf("path = %q, want db.sdf", path)
	}
	if password != "from-dsn" {
		t.Fatalf("password = %q, want DSN password", password)
	}
}

func TestParseDSNExplicitEmptyPasswordDoesNotFallBackToEnv(t *testing.T) {
	path, password := parseDSNWithEnv("db.sdf?password=", func(string) string {
		return "from-env"
	})

	if path != "db.sdf" {
		t.Fatalf("path = %q, want db.sdf", path)
	}
	if password != "" {
		t.Fatalf("password = %q, want empty password", password)
	}
}

func TestParseDSNUsesEnvWhenQueryHasNoPasswordParameter(t *testing.T) {
	path, password := parseDSNWithEnv("db.sdf?mode=ro", func(string) string {
		return "from-env"
	})

	if path != "db.sdf" {
		t.Fatalf("path = %q, want db.sdf", path)
	}
	if password != "from-env" {
		t.Fatalf("password = %q, want env password", password)
	}
}
