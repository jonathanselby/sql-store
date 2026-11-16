package sqlstore_test

import (
	"os"
	"strings"
	"testing"
)

func TestDependencyBoundary(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, forbidden := range []string{"gorm.io", "entgo.io", "jmoiron/sqlx", "Masterminds/squirrel", "sqlboiler"} {
		if strings.Contains(s, forbidden) {
			t.Fatalf("forbidden data-access framework: %s", forbidden)
		}
	}
}
