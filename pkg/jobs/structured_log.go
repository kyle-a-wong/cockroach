// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// LogStateChangeStructured job state change using structured logging, if job is not nil.
func LogStateChangeStructured(
	ctx context.Context,
	id jobspb.JobID,
	payload *jobspb.Payload,
	runStats *RunStats,
	prevStatus Status,
	status Status,
) {
	if payload == nil {
		return
	}

	var runNum int
	if runStats != nil {
		runNum = runStats.NumRuns
	}

	out := eventpb.StatusChange{
		JobID:          int64(id),
		JobType:        payload.Type().String(),
		Description:    fmt.Sprintf("status changed to: %s", status),
		PreviousStatus: string(prevStatus),
		NewStatus:      string(status),
		RunNum:         int32(runNum),
	}

	if payload.FinalResumeError != nil {
		out.FinalResumeErr = payload.FinalResumeError.String()
	}

	if payload.Error != "" {
		out.Error = payload.Error
	}

	log.InfoE(ctx, &out)
}
