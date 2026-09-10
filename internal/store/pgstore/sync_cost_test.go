package pgstore_test

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/pgstore"
)

type syncCostKey struct {
	owner string
	conv  string
}

type syncCostHead struct {
	min  int64
	max  int64
	read int64
}

type syncCostQueries struct{ count atomic.Int64 }

func (q *syncCostQueries) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	q.count.Add(1)
	return ctx
}

func (*syncCostQueries) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// This compares existing services with a test-only batch query, not a deployed sync implementation.
// Every iteration scans all fixture users; no HTTP, concurrent sends, ACKs or socket fanout are measured.
func BenchmarkSyncCost(b *testing.B) {
	dsn := os.Getenv("NEXO_TEST_PG_DSN")
	if dsn == "" || os.Getenv("NEXO_TEST_DISPOSABLE") != "1" {
		b.Skip("requires NEXO_TEST_PG_DSN and NEXO_TEST_DISPOSABLE=1; replaces fixture tables")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		b.Fatal(err)
	}
	queries := &syncCostQueries{}
	cfg.MaxConns = 4
	cfg.ConnConfig.Tracer = queries
	pool, err := pgxpool.NewWithConfig(b.Context(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	customCfg := cfg.Copy()
	customCfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_custom_plan"
	customPool, err := pgxpool.NewWithConfig(b.Context(), customCfg)
	if err != nil {
		b.Fatal(err)
	}
	defer customPool.Close()
	st := pgstore.FromPool(pool)
	msgs := message.New(message.Adapt(st), message.NoopPublisher{}, message.Config{MaxContentBytes: 8192})
	convs := conversation.New(st, conversation.NoopNotifier{})

	for _, users := range []int{100, 1000} {
		b.Run(fmt.Sprintf("users=%d/conversations=250", users), func(b *testing.B) {
			seedSyncCost(b, pool, users)
			owners := make([]string, users)
			for i := range users {
				owners[i] = fmt.Sprintf("u___%d", i+1)
			}
			current := func(b *testing.B, withList bool) map[syncCostKey]syncCostHead {
				b.Helper()
				out := make(map[syncCostKey]syncCostHead, users*250)
				for _, owner := range owners {
					cursor := ""
					for {
						page, err := msgs.MaxSeqs(b.Context(), owner, cursor, 200, 200)
						if err != nil {
							b.Fatal(err)
						}
						for _, item := range page.Items {
							out[syncCostKey{owner: owner, conv: item.ConversationId}] = syncCostHead{
								min: item.MinSeq, max: item.MaxSeq, read: item.ReadSeq,
							}
						}
						if !page.HasMore {
							break
						}
						cursor = page.NextCursor
					}
					if !withList {
						continue
					}
					cursor = ""
					for {
						page, err := convs.List(b.Context(), owner, cursor, 100, 100, true)
						if err != nil {
							b.Fatal(err)
						}
						if !page.HasMore {
							break
						}
						cursor = page.NextCursor
					}
				}
				return out
			}
			expected := current(b, false)
			if len(expected) != users*250 {
				b.Fatal("incomplete MaxSeqs baseline")
			}
			for _, query := range []string{syncCostBatchQuery, syncCostBoundedQuery} {
				if !maps.Equal(expected, batchSyncCost(b, pool, owners, query)) {
					b.Fatalf("batch heads differ from MaxSeqs, including visibility boundaries: %s", query)
				}
			}
			if !maps.Equal(expected, batchSyncCost(b, customPool, owners, syncCostBoundedQuery)) {
				b.Fatal("custom-plan batch differs from MaxSeqs")
			}
			_ = current(b, true)
			if os.Getenv("NEXO_SYNC_EXPLAIN") == "1" {
				explainSyncCost(b, pool, owners)
			}
			cases := []struct {
				name string
				run  func(*testing.B) map[syncCostKey]syncCostHead
			}{
				{name: "max_seqs", run: func(b *testing.B) map[syncCostKey]syncCostHead { return current(b, false) }},
				{name: "max_seqs_and_list", run: func(b *testing.B) map[syncCostKey]syncCostHead { return current(b, true) }},
				{name: "batch_heads_prototype", run: func(b *testing.B) map[syncCostKey]syncCostHead {
					return batchSyncCost(b, pool, owners, syncCostBatchQuery)
				}},
				{name: "batch_heads_bounded_prototype", run: func(b *testing.B) map[syncCostKey]syncCostHead {
					return batchSyncCost(b, pool, owners, syncCostBoundedQuery)
				}},
				{name: "batch_heads_bounded_custom_prototype", run: func(b *testing.B) map[syncCostKey]syncCostHead {
					return batchSyncCost(b, customPool, owners, syncCostBoundedQuery)
				}},
			}
			for _, tc := range cases {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					before := queries.count.Load()
					var got map[syncCostKey]syncCostHead
					for b.Loop() {
						got = tc.run(b)
					}
					b.ReportMetric(float64(queries.count.Load()-before)/float64(b.N), "queries/round")
					b.ReportMetric(float64(len(got)), "heads/round")
					if !maps.Equal(expected, got) {
						b.Fatal("incomplete or inconsistent sync round")
					}
				})
			}
		})
	}
}

const syncCostBatchQuery = `
SELECT uc.owner_id, uc.conversation_id, uc.min_seq,
       CASE WHEN uc.max_seq = 0 THEN c.max_seq ELSE LEAST(uc.max_seq, c.max_seq) END,
       uc.read_seq
FROM user_conversations uc JOIN conversations c USING (conversation_id)
WHERE uc.owner_id = ANY($1::text[])
  AND (uc.owner_id, uc.conversation_id) > ($2::text, $3::text)
ORDER BY uc.owner_id, uc.conversation_id
LIMIT 2000`

// The tuple bound already implies this scalar bound; expose it to the leading B-tree column.
var syncCostBoundedQuery = strings.Replace(syncCostBatchQuery, "\nORDER BY", "\n  AND uc.owner_id >= $2::text\nORDER BY", 1)

func batchSyncCost(b *testing.B, pool *pgxpool.Pool, owners []string, query string) map[syncCostKey]syncCostHead {
	b.Helper()
	out := make(map[syncCostKey]syncCostHead, len(owners)*250)
	for ids := range slices.Chunk(owners, 100) {
		owner, conv := "", ""
		for {
			rows, err := pool.Query(b.Context(), query, ids, owner, conv)
			if err != nil {
				b.Fatal(err)
			}
			n := 0
			for rows.Next() {
				var head syncCostHead
				if err := rows.Scan(&owner, &conv, &head.min, &head.max, &head.read); err != nil {
					rows.Close()
					b.Fatal(err)
				}
				out[syncCostKey{owner: owner, conv: conv}] = head
				n++
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				b.Fatal(err)
			}
			if n < 2000 {
				break
			}
		}
	}
	return out
}

func explainSyncCost(b *testing.B, pool *pgxpool.Pool, owners []string) {
	b.Helper()
	conn, err := pool.Acquire(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Release()
	defer func() {
		if _, err := conn.Exec(b.Context(), "RESET plan_cache_mode"); err != nil {
			b.Error(err)
		}
	}()
	rows, err := conn.Query(b.Context(), "SELECT name, statement, generic_plans, custom_plans FROM pg_prepared_statements")
	if err != nil {
		b.Fatal(err)
	}
	statements := map[string]string{}
	for rows.Next() {
		var name, sql string
		var generic, custom int64
		if err := rows.Scan(&name, &sql, &generic, &custom); err != nil {
			rows.Close()
			b.Fatal(err)
		}
		switch {
		case sql == syncCostBatchQuery:
			statements["batch"] = name
			b.Logf("SYNC_PREPARED batch generic=%d custom=%d", generic, custom)
		case sql == syncCostBoundedQuery:
			statements["batch_bounded"] = name
			b.Logf("SYNC_PREPARED batch_bounded generic=%d custom=%d", generic, custom)
		case strings.Contains(sql, "ORDER BY uc.updated_at DESC, uc.conversation_id DESC"):
			statements["max_seqs"] = name
			b.Logf("SYNC_PREPARED max_seqs generic=%d custom=%d", generic, custom)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
	if len(statements) != 3 {
		b.Fatalf("missing warmed prepared statements: %v", statements)
	}

	ids := owners[:min(len(owners), 100)]
	starts := []syncCostKey{}
	cursor := syncCostKey{}
	for {
		starts = append(starts, cursor)
		rows, err := conn.Query(b.Context(), syncCostBatchQuery, ids, cursor.owner, cursor.conv)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			var head syncCostHead
			if err := rows.Scan(&cursor.owner, &cursor.conv, &head.min, &head.max, &head.read); err != nil {
				rows.Close()
				b.Fatal(err)
			}
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		if n < 2000 {
			break
		}
	}

	// EXPLAIN the already-warmed statements, not a separate query planned only for the sample values.
	explain := func(label, name string, args ...any) {
		params := make([]string, len(args))
		for i := range params {
			params[i] = fmt.Sprintf("$%d", i+1)
		}
		sql := fmt.Sprintf("EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) EXECUTE %s(%s)",
			pgx.Identifier{name}.Sanitize(), strings.Join(params, ","))
		// Simple protocol lets pgx safely encode EXECUTE arguments as literals.
		args = append([]any{pgx.QueryExecModeSimpleProtocol}, args...)
		var plan []byte
		if err := conn.QueryRow(b.Context(), sql, args...).Scan(&plan); err != nil {
			b.Fatal(err)
		}
		b.Logf("SYNC_PLAN %s %s", label, strings.ReplaceAll(string(plan), "\n", ""))
	}
	first := store.FirstPage()
	for _, mode := range []string{"auto", "force_custom_plan", "force_generic_plan"} {
		if _, err := conn.Exec(b.Context(), "SET plan_cache_mode = "+mode); err != nil {
			b.Fatal(err)
		}
		explain(mode+"/max_seqs/first", statements["max_seqs"], ids[0], first.UpdatedAt, first.ConversationId, 201)
		for _, kind := range []string{"batch", "batch_bounded"} {
			for _, i := range []int{0, len(starts) / 2, len(starts) - 1} {
				explain(fmt.Sprintf("%s/%s/page=%d", mode, kind, i+1),
					statements[kind], ids, starts[i].owner, starts[i].conv)
			}
		}
	}
}

func seedSyncCost(b *testing.B, pool *pgxpool.Pool, users int) {
	b.Helper()
	// The caller requires explicit disposable opt-in; use only the test-all throwaway database.
	if _, err := pool.Exec(b.Context(), "TRUNCATE messages, user_conversations, conversations, users"); err != nil {
		b.Fatal(err)
	}
	statements := []string{
		`INSERT INTO users (id) SELECT 'u___' || u FROM generate_series(1, $1::int) u`,
		`INSERT INTO conversations (conversation_id, type, max_seq)
SELECT 'si:ag__' || c || ':u___' || u, 1, 2
FROM generate_series(1, $1::int) u CROSS JOIN generate_series(1, 250) c`,
		`INSERT INTO user_conversations
(owner_id, conversation_id, type, peer_user_id, min_seq, max_seq, read_seq, updated_at)
SELECT 'u___' || u, 'si:ag__' || c || ':u___' || u, 1, 'ag__' || c,
       CASE WHEN u % 10 = 1 THEN 2 ELSE 1 END,
       CASE WHEN u % 10 = 0 THEN 1 ELSE 0 END,
       CASE WHEN u % 5 = 0 THEN 1 ELSE 0 END,
       '2026-01-01'::timestamptz + (c % 7) * interval '1 millisecond'
FROM generate_series(1, $1::int) u CROSS JOIN generate_series(1, 250) c`,
		`INSERT INTO messages
(conversation_id, seq, server_msg_id, client_msg_id, sender_id, recv_id, session_type, content_type, content)
SELECT 'si:ag__' || c || ':u___' || u, seq, 'cost-' || u || '-' || c || '-' || seq,
       'cost-' || seq, 'ag__' || c, 'u___' || u, 1, 101,
       '{"text":"' || repeat('x', 256) || '"}'
FROM generate_series(1, $1::int) u CROSS JOIN generate_series(1, 250) c CROSS JOIN generate_series(1, 2) seq`,
	}
	for _, sql := range statements {
		if _, err := pool.Exec(b.Context(), sql, users); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := pool.Exec(b.Context(), "ANALYZE user_conversations; ANALYZE conversations; ANALYZE messages"); err != nil {
		b.Fatal(err)
	}
}
