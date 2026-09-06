package gormstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/samber/lo"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore/model"
)

type Store struct {
	db     *gorm.DB
	shared bool // the connection belongs to someone else: Close leaves it open
	inTx   bool // transaction-scoped view handed to a WithTx callback
}

var _ store.Store = (*Store)(nil)

// checkMysqlDsn rejects connection flags that break InsertMessage's duplicate detection.
//
// GORM renders clause.OnConflict{DoNothing} on MySQL as "ON DUPLICATE KEY UPDATE col=col", so a
// duplicate is only distinguishable by the statement changing no rows. CLIENT_FOUND_ROWS makes
// MySQL report matched rows instead of changed ones, and that no-op update then counts as one:
// InsertMessage answers inserted=true for a message it did not write, the service skips its
// rollback and advances max_seq past a seq that has no row behind it. Every retry burns another
// sequence number and acks a server_msg_id no client can ever pull.
//
// Parse rather than match text: the driver accepts 1, t, T, true and TRUE for the same flag.
func checkMysqlDsn(dsn string) error {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("gormstore: mysql dsn: %w", err)
	}
	if cfg.ClientFoundRows {
		return errors.New("gormstore: mysql dsn sets clientFoundRows, which makes duplicate messages look inserted; remove it")
	}
	// gorm.io/driver/mysql does not set this for us: without it datetime(3) comes back as []byte
	// and every time.Time scan fails, so the process boots clean and then serves nothing but 20001.
	if !cfg.ParseTime {
		return errors.New("gormstore: mysql dsn must set parseTime=true, or every datetime(3) scan fails")
	}
	return nil
}

func New(driver, dsn string, maxOpen int) (*Store, error) {
	if driver == "mysql" {
		if err := checkMysqlDsn(dsn); err != nil {
			return nil, err
		}
	}
	dialector := lo.Ternary(driver == "mysql", mysql.Open(dsn), postgres.Open(dsn))
	db, err := gorm.Open(dialector, &gorm.Config{
		TranslateError: true,
		Logger:         logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("gormstore: %w", err)
	}
	sqlDb, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("gormstore: %w", err)
	}
	sqlDb.SetMaxOpenConns(maxOpen)
	return &Store{db: db}, nil
}

// FromDb wraps a host-owned connection; Close then leaves it open. The DSN is the host's, so
// checkMysqlDsn cannot run here: a host handing over a MySQL pool must not enable CLIENT_FOUND_ROWS.
func FromDb(db *gorm.DB) *Store { return &Store{db: db, shared: true} }

func (s *Store) Ping(ctx context.Context) error {
	sqlDb, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDb.PingContext(ctx)
}

func (s *Store) Close() {
	if s.shared {
		return
	}
	if sqlDb, err := s.db.DB(); err == nil {
		sqlDb.Close()
	}
}

func (s *Store) WithTx(ctx context.Context, fn func(store.Store) error) error {
	// GORM would nest this as a SAVEPOINT, which pgstore cannot do: refuse in both so the
	// backends agree (store.Store forbids nesting).
	if s.inTx {
		return store.ErrNestedTx
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The callback borrows the connection; shared keeps Close from closing the pooled
		// *sql.DB under every other in-flight request.
		return fn(&Store{db: tx, shared: true, inTx: true})
	})
}

func wrap(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrRecordNotFound):
		return store.ErrNotFound
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return store.ErrDuplicate
	default:
		return err
	}
}

func (s *Store) GetUser(ctx context.Context, id string) (*store.User, error) {
	u, err := gorm.G[model.User](s.db).Where("id = ?", id).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.User(u)), nil
}

func (s *Store) GetUsers(ctx context.Context, ids []string) ([]store.User, error) {
	rows, err := gorm.G[model.User](s.db).Where("id IN ?", ids).Find(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return lo.Map(rows, func(u model.User, _ int) store.User { return store.User(u) }), nil
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*store.User, error) {
	u, err := gorm.G[model.User](s.db).Where("username = ?", username).First(ctx)
	if err != nil {
		return nil, wrap(err)
	}
	return new(store.User(u)), nil
}

func (s *Store) CreateUser(ctx context.Context, u *store.User) error {
	return wrap(gorm.G[model.User](s.db).Create(ctx, new(model.User(*u))))
}

func (s *Store) UpdateUserProfile(ctx context.Context, id string, nickname, avatar, extra *string, now time.Time) error {
	sets := []clause.Assigner{clause.Assignment{Column: clause.Column{Name: "updated_at"}, Value: now}}
	// Ordered, not a map: the column order decides the statement text, and a stable text is what
	// lets the driver and the server reuse one prepared statement / plan for every call.
	for _, f := range []struct {
		col string
		val *string
	}{{"nickname", nickname}, {"avatar", avatar}, {"extra", extra}} {
		if f.val != nil {
			sets = append(sets, clause.Assignment{Column: clause.Column{Name: f.col}, Value: *f.val})
		}
	}
	n, err := gorm.G[model.User](s.db).Where("id = ?", id).Set(sets...).Update(ctx)
	if err == nil && n == 0 {
		// MySQL counts changed rows, not matched rows; a same-millisecond retry can be a no-op.
		_, err = s.GetUser(ctx, id)
	}
	return wrap(err)
}

func (s *Store) UpsertUser(ctx context.Context, u *store.User) error {
	row := model.User{Id: u.Id, Nickname: u.Nickname, Avatar: u.Avatar, Extra: u.Extra, CreatedAt: u.UpdatedAt, UpdatedAt: u.UpdatedAt}
	onConflict := clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"nickname", "avatar", "extra", "updated_at"}),
	}
	return wrap(gorm.G[model.User](s.db, onConflict).Create(ctx, &row))
}
