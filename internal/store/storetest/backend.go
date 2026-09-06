package storetest

import (
	"os"
	"testing"

	"github.com/mbeoliero/nexo/internal/store"
)

// Backend names one store implementation a service suite runs against. Driver is empty for the
// sqlc-postgres store and for the in-memory one; Env is empty only for the in-memory one.
type Backend struct {
	Name   string
	Driver string
	Env    string
}

func Backends() []Backend {
	return []Backend{
		{Name: "memory"},
		{Name: "gorm-postgres", Driver: "postgres", Env: "NEXO_TEST_PG_DSN"},
		{Name: "gorm-mysql", Driver: "mysql", Env: "NEXO_TEST_MYSQL_DSN"},
		{Name: "sqlc-postgres", Env: "NEXO_TEST_PG_DSN"},
	}
}

// DbBackends drops the in-memory store, for suites that only assert database behaviour.
func DbBackends() []Backend { return Backends()[1:] }

// Open returns the store for b and registers its cleanup, skipping when the backend's DSN is
// unset. The caller supplies newDb because storetest cannot import gormstore or pgstore: their
// own suites are in-package tests that import storetest.
func Open(t *testing.T, b Backend, newDb func(driver, dsn string) (store.Store, error)) store.Store {
	t.Helper()
	st := store.Store(NewMem())
	if b.Env != "" {
		dsn := os.Getenv(b.Env)
		if dsn == "" {
			t.Skip(b.Env + " not set")
		}
		var err error
		if st, err = newDb(b.Driver, dsn); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(st.Close)
	return st
}
