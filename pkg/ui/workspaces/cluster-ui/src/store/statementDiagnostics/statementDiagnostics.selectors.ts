// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";
import orderBy from "lodash/orderBy";
import groupBy from "lodash/groupBy";
import mapValues from "lodash/mapValues";
import moment from "moment-timezone";

import { AppState } from "../reducers";
import { StatementDiagnosticsReport } from "../../api";

export const statementDiagnostics = createSelector(
  (state: AppState) => state.adminUI,
  state => state?.statementDiagnostics,
);

export const selectStatementDiagnosticsReports = createSelector(
  statementDiagnostics,
  state => state.data,
);

export type StatementDiagnosticsDictionary = {
  [statementFingerprint: string]: StatementDiagnosticsReport[];
};

export const selectDiagnosticsReportsPerStatement = createSelector(
  selectStatementDiagnosticsReports,
  (
    diagnosticsReports: StatementDiagnosticsReport[],
  ): StatementDiagnosticsDictionary => {
    const diagnosticsPerFingerprint = groupBy(
      diagnosticsReports,
      diagnosticsReport => diagnosticsReport.statement_fingerprint,
    );
    return mapValues(diagnosticsPerFingerprint, diagnostics =>
      orderBy(diagnostics, [d => moment(d.requested_at).unix()], ["desc"]),
    );
  },
);

export const selectDiagnosticsReportsByStatementFingerprint = createSelector(
  selectStatementDiagnosticsReports,
  (_state: AppState, statementFingerprint: string) => statementFingerprint,
  (requests, statementFingerprint) =>
    (requests || []).filter(
      request => request.statement_fingerprint === statementFingerprint,
    ),
);
