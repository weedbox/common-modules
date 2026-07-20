package postgres_connector

import "testing"

func TestResolveSSLMode(t *testing.T) {
	valid := map[string]string{
		// Boolean spellings (the key's historical type). viper renders
		// bool config values as "true"/"false" via GetString.
		"false": "disable",
		"true":  "require",
		"":      "disable",
		"0":     "disable",
		"1":     "require",
		"off":   "disable",
		"on":    "require",
		"no":    "disable",
		"yes":   "require",
		// The invalid value the old code generated — kept as a forgiving
		// alias so anyone who copied it into config gets working SSL.
		"enable": "require",
		// Real libpq modes pass through.
		"disable":     "disable",
		"allow":       "allow",
		"prefer":      "prefer",
		"require":     "require",
		"verify-ca":   "verify-ca",
		"verify-full": "verify-full",
		// Case/whitespace tolerance.
		"TRUE":          "require",
		" Verify-Full ": "verify-full",
	}
	for in, want := range valid {
		got, err := resolveSSLMode(in)
		if err != nil {
			t.Errorf("resolveSSLMode(%q): unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("resolveSSLMode(%q): got %q, want %q", in, got, want)
		}
	}

	// Unknown values must error so typos surface at startup instead of
	// silently downgrading SSL.
	for _, in := range []string{"enabled", "ssl", "verify", "requier"} {
		if _, err := resolveSSLMode(in); err == nil {
			t.Errorf("resolveSSLMode(%q): want error, got nil", in)
		}
	}
}
