// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hotspots

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

type hotspotInsertBigINT struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	ranges        int
	isDistributed bool
}

func init() {
	workload.Register(hotspotInsertBigINTMeta)
}

var RandomSeed = workload.NewUint64RandomSeed()

var hotspotInsertBigINTMeta = workload.Meta{
	Name:        `hotspots__insert_bigint`,
	Description: `Simulate workload similar to hotspot workloads for comparison`,
	Version:     `1.0.0`,
	RandomSeed:  RandomSeed,
	New: func() workload.Generator {
		g := &hotspotInsertBigINT{}
		g.flags.FlagSet = pflag.NewFlagSet(`hotspots`, pflag.ContinueOnError)
		g.flags.IntVar(&g.ranges, `num-ranges`, 64, `Initial number of ranges to break the users table into.`)
		g.flags.BoolVar(&g.isDistributed, `is-distributed`, false, `Whether the inserts distribute evenly in the keyspace.`)
		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

var _ workload.Generator = &hotspotInsertBigINT{}
var _ workload.Opser = &hotspotInsertBigINT{}
var _ workload.Hookser = &hotspotInsertBigINT{}

// Meta implements the Generator interface.
func (*hotspotInsertBigINT) Meta() workload.Meta { return hotspotInsertBigINTMeta }

// Flags implements the Flagser interface.
func (w *hotspotInsertBigINT) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *hotspotInsertBigINT) ConnFlags() *workload.ConnFlags { return w.connFlags }

// Hooks implements the Hookser interface.
func (w *hotspotInsertBigINT) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *hotspotInsertBigINT) Tables() []workload.Table {
	spacerColumn := func(i int) string {
		return fmt.Sprintf("s%d VARCHAR(32) DEFAULT MD5(RANDOM()::TEXT)", i)
	}
	columns := []string{"id BIGINT PRIMARY KEY"}
	for i := 0; i < 10; i++ {
		columns = append(columns, spacerColumn(i))
	}

	return []workload.Table{{
		Name:   `users`,
		Schema: fmt.Sprintf("(%s)", strings.Join(columns, ",")),
		Splits: workload.Tuples(w.ranges, func(i int) []interface{} {
			offset := float64(i) / float64(w.ranges)
			return []interface{}{offset * math.MaxInt64}
		}),
	}}
}

// Ops implements the Opser interface.
func (w *hotspotInsertBigINT) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			return db.Close()
		},
	}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			start := timeutil.Now()

			updateStmt, err := db.Prepare(w.Query())
			if err != nil {
				return errors.CombineErrors(err, db.Close())
			}

			_, err = updateStmt.Exec()
			elapsed := timeutil.Since(start)
			hists.Get(`transfer`).Record(elapsed)
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}

func (w *hotspotInsertBigINT) Query() string {
	if w.isDistributed {
		return fmt.Sprintf("INSERT INTO users (id) VALUES (RANDOM * %d", math.MaxInt64)
	} else {
		return "INSERT INTO users (id) VALUES (unique_rowid())"
	}
}
