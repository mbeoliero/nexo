package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/pgstore/gen"
)

type Store struct {
	pool   *pgxpool.Pool
	q      *gen.Queries
	shared bool // the pool belongs to someone else: Close leaves it open
	inTx   bool // transaction-scoped view handed to a WithTx callback
}

var _ store.Store = (*Store)(nil)

func New(ctx context.Context, dsn string, maxConns int) (*Store, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	pc.MaxConns = int32(maxConns)
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	return &Store{pool: pool, q: gen.New(pool)}, nil
}

// FromPool wraps a host-owned pool; Close then leaves it open.
func FromPool(pool *pgxpool.Pool) *Store { return &Store{pool: pool, q: gen.New(pool), shared: true} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Close() {
	if !s.shared {
		s.pool.Close()
	}
}

func (s *Store) WithTx(ctx context.Context, fn func(store.Store) error) error {
	// Begin here would open a second, independent transaction on another connection, and its
	// writes would commit whatever the outer one does.
	if s.inTx {
		return store.ErrNestedTx
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin: %w", err)
	}
	defer tx.Rollback(ctx)
	// The callback borrows both the pool and the tx; shared keeps Close from tearing the
	// process-wide pool down under every other in-flight request.
	if err := fn(&Store{pool: s.pool, q: s.q.WithTx(tx), shared: true, inTx: true}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "23505" {
		return store.ErrDuplicate
	}
	return err
}

func (s *Store) GetUser(ctx context.Context, id string) (*store.User, error) {
	u, err := s.q.GetUser(ctx, id)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.User(u)), nil
}

func (s *Store) GetUsers(ctx context.Context, ids []string) ([]store.User, error) {
	rows, err := s.q.GetUsers(ctx, ids)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(u gen.User, _ int) store.User { return store.User(u) }), nil
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*store.User, error) {
	u, err := s.q.GetUserByUsername(ctx, username)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.User(u)), nil
}

func (s *Store) CreateUser(ctx context.Context, u *store.User) error {
	return wrap(s.q.CreateUser(ctx, gen.CreateUserParams{
		Id:           u.Id,
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		Nickname:     u.Nickname,
		Avatar:       u.Avatar,
		Extra:        u.Extra,
		CreatedAt:    u.CreatedAt,
		UpdatedAt:    u.UpdatedAt,
	}))
}

func (s *Store) UpdateUserProfile(ctx context.Context, id string, nickname, avatar, extra *string, now time.Time) error {
	n, err := s.q.UpdateUserProfile(ctx, gen.UpdateUserProfileParams{Id: id, Nickname: nickname, Avatar: avatar, Extra: extra, UpdatedAt: now})
	if err == nil && n == 0 {
		return store.ErrNotFound
	}
	return wrap(err)
}

func (s *Store) UpsertUser(ctx context.Context, u *store.User) error {
	return wrap(s.q.UpsertUser(ctx, gen.UpsertUserParams{
		Id:        u.Id,
		Nickname:  u.Nickname,
		Avatar:    u.Avatar,
		Extra:     u.Extra,
		CreatedAt: u.UpdatedAt,
	}))
}
