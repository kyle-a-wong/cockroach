// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerDbConsole(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "db-console/login",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(1),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Randomized:       false,
		Run:              runDbConsoleLogin,
		Timeout:          1 * time.Hour,
	})
}

func runDbConsoleLogin(ctx context.Context, t test.Test, c cluster.Cluster) {
	t.Status("setting up cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	node := c.Node(1)
	db, err := c.ConnE(ctx, t.L(), node[0])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, cmd := range []string{
		`CREATE USER cypress PASSWORD 'tests'`,
		`GRANT admin TO cypress`,
	} {
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}

	version, _ := fetchCockroachVersion(ctx, t.L(), c, node[0])
	t.L().Printf("cockroach version: %s", version)

	internalAdminUIAddrs, _ := c.InternalAdminUIAddr(ctx, t.L(), node)
	internalAdminUiUrl := fmt.Sprintf("https://%s/api/v2/login/?username=cypress&password=tests", internalAdminUIAddrs[0])
	t.L().Printf("internalAdminUiUrl: %s", internalAdminUiUrl)

	adminUIAddrs, _ := c.ExternalAdminUIAddr(ctx, t.L(), node)
	adminUiUrl := fmt.Sprintf("https://%s/api/v2/login/?username=cypress&password=tests", adminUIAddrs[0])
	t.L().Printf("adminUiUrl: %s", adminUiUrl)
	out1, _ := exec.Command("curl", "--request", "POST", "-k", internalAdminUiUrl).Output()
	sOut := string(out1)
	t.L().Printf("internal OUTPUT: %s", sOut)

	out2, _ := exec.Command("curl", "--request", "POST", "-k", adminUiUrl).Output()
	sOut2 := string(out2)
	t.L().Printf("external OUTPUT: %s", sOut2)

	require.False(t, strings.Contains(sOut, "internal server error") && strings.Contains(sOut2, "internal server error"))
}
