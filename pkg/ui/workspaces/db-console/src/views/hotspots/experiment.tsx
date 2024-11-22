import React, { useEffect, useState } from "react";
import { useDispatch, useSelector } from "react-redux";

import * as protos from "src/js/protos";
import { hotRangesSelector, isValidSelector } from "oss/src/redux/hotRanges";
import { cockroach } from "src/js/protos";
import { refreshHotRanges } from "src/redux/apiReducers";
import {
  util,
} from "@cockroachlabs/cluster-ui";


import {
  MetricsDataComponentProps,
} from "../shared/components/metricQuery";
import {
  SummaryMetricsAggregator,
  aggregateLatestValuesFromMetrics,
} from "../shared/components/summaryBar";
import {
  current,
} from "../shared/containers/metricDataProvider";
import { nodeIDsSelector } from "oss/src/redux/nodes";
import {
  queryTimeSeries,
} from "oss/src/util/api";

const HotRangesRequest = cockroach.server.serverpb.HotRangesRequest;
const TSAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator;
const TSDerivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative;
const TimeSeriesQueryRequest = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;
type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;

function mean(array: number[]) {
  return array.reduce((a: number, b: number) => a + b) / array.length;
}

function stddev(array: number[]) {
  const n = array.length;
  const avg = mean(array);
  return Math.sqrt(
    array
      .map(x => Math.pow(x - avg, 2))
      .reduce((a: number, b: number) => a + b) / n,
  );
}

function coefficientOfVariation(array: number[]) {
  return stddev(array) / mean(array);
}

interface Datapoint {
  node: number,
  qps: number;
  cpu: number;
  write_bytes: number;
}

interface cell {
  identifier: string;
  value: string;
}

function hotRangesToDatapoints(ranges: HotRange[]): Datapoint[] {
  return ranges.map((range: HotRange) => ({
    node: range.leaseholder_node_id,
    qps: range.qps,
    cpu: range.cpu_time_per_second,
    write_bytes: range.write_bytes_per_second,
  }));
}


async function getMiscData(): Promise<cell[]> {
  const timeInfo = current();
  const queries = [
    {
      name: "cr.node.sql.crud_query.count",
      downsampler: TSAggregator.SUM,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
      aggregator: TSAggregator.SUM,
    },
    {
      name: "kv.concurrency.avg_lock_wait_duration_nanos",
      downsampler: TSAggregator.MAX,
      aggregator: TSAggregator.MAX,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
    },
    {
      name: "kv.concurrency.avg_lock_wait_duration_nanos",
      downsampler: TSAggregator.AVG,
      aggregator: TSAggregator.AVG,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
    },
    {
      name: "kv.concurrency.latch_conflict_wait_durations",
      downsampler: TSAggregator.MAX,
      aggregator: TSAggregator.MAX,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
    },
    {
      name: "kv.concurrency.latch_conflict_wait_durations",
      downsampler: TSAggregator.AVG,
      aggregator: TSAggregator.AVG,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
    },
    {
      name: "cr.node.sys.runnable.goroutines.per.cpu",
      downsampler: TSAggregator.MAX,
      aggregator: TSAggregator.MAX,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
    },
    {
      name: "cr.node.sys.runnable.goroutines.per.cpu",
      downsampler: TSAggregator.AVG,
      aggregator: TSAggregator.AVG,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
    },
  ];
  const data = await queryTimeSeries(
    new TimeSeriesQueryRequest({
      queries: queries,
      start_nanos: timeInfo.start,
      end_nanos: timeInfo.end,
      sample_nanos: timeInfo.sampleDuration,
    }),
  );
  return data.results.map((result) => {
    let identifier = '';
    switch (result.query.name) {
      case "cr.node.sql.crud_query.count":
        identifier = "Cluster QPS";
        break;
      case "kv.concurrency.avg_lock_wait_duration_nanos":
        identifier = "Lock Wait"; break; case "kv.concurrency.latch_conflict_wait_durations": identifier = "Latch Wait"; break;
      case "cr.node.sys.runnable.goroutines.per.cpu":
        identifier = "Runnable Goroutines per CPU";
        break;
      default:
        throw new Error('i dont know this type ' + result.query.name);
    }
    identifier += ` (${TSAggregator[result.query.downsampler]})`;
    const value = result.datapoints.length ? result.datapoints[0].value.toFixed(3) : "missing"
    return { identifier, value };
  });
}

async function getNodeDatapoints(nodeIDs: number[]): Promise<Datapoint[]> {
  if (!nodeIDs?.length) {
    return;
  }
  const timeInfo = current();
  const sources = nodeIDs.map(id => id.toString());
  const queries = sources
    .map(id => ({
      name: "cr.node.sql.crud_query.count",
      downsampler: TSAggregator.SUM,
      derivative: TSDerivative.NON_NEGATIVE_DERIVATIVE,
      sources: [id],
    }))
    .concat(
      sources.map(id => ({
        name: "cr.node.sys.cpu.host.combined.percent-normalized",
        downsampler: TSAggregator.AVG,
        derivative: TSDerivative.NONE,
        sources: [id],
      })),
    )
    .concat(
      sources.map(id => ({
        name: "cr.node.sys.host.disk.write.bytes",
        downsampler: TSAggregator.AVG,
        derivative: TSDerivative.NONE,
        sources: [id],
      })),
    );
  const data = await queryTimeSeries(
    new TimeSeriesQueryRequest({
      queries: queries,
      start_nanos: timeInfo.start,
      end_nanos: timeInfo.end,
      sample_nanos: timeInfo.sampleDuration,
    }),
  );
  const nodeData: Record<number, Datapoint> = {};
  for (const result of data.results) {
    const nodeId = parseInt(result.query.sources[0]);
    if (!nodeData[nodeId]) {
      nodeData[nodeId] = {} as Datapoint;
    }
    switch (result.query.name) {
      case "cr.node.sql.crud_query.count":
        nodeData[nodeId].qps = result.datapoints[0].value;
        break;
      case "cr.node.sys.cpu.host.combined.percent-normalized":
        nodeData[nodeId].cpu = result.datapoints[0].value;
        break;
      case "cr.node.sys.host.disk.write.bytes":
        nodeData[nodeId].write_bytes = result.datapoints[0].value;
        break;
      default:
        throw new Error('i dont know this type ' + result.query.name);
    }
  }
  return Object.entries(nodeData).map(([_, v]) => v);
}


function collectResults(nodes: Datapoint[], nodeRanges: Datapoint[], allRanges: Datapoint[]): cell[] {
  const results = [];
  for (const [datasetID, dataset] of [
    ["N", nodes],
    ["L", nodeRanges],
    ["G", allRanges],
  ]) {
    if (!dataset?.length) {
      continue;
    }
    for (const [aggregator, aggFn] of [
      ["M", (numbers: number[]) => Math.max(...numbers)],
      ["A", mean],
      ["Cv", coefficientOfVariation],
    ]) {
      for (const [metric, metricKey] of [
        ["Q", "qps"],
        ["C", "cpu"],
        ["W", "write_bytes"],
      ]) {
        const identifier = datasetID as string + aggregator + metric;
        const numbers = (dataset as Datapoint[]).map((range: any) => range[metricKey]);
        const valueNum = (aggFn as any)(numbers);
        let value = "";
        if (datasetID !== "N" && metric === "C") {
          value = util.Duration(valueNum)
        } else {
          value = (aggFn as any)(numbers).toFixed(3);
        }
        results.push({ identifier, value });
      }
    }
  }
  return results;
}

const HotspotsExperiment = () => {
  const dispatch = useDispatch();
  const hotRanges = useSelector(hotRangesSelector);
  const isValid = useSelector(isValidSelector);
  const nodeIds = useSelector(nodeIDsSelector);
  const [nodeData, setNodeData] = useState<Datapoint[]>([]);
  const [miscData, setMiscData] = useState<cell[]>([]);
  useEffect(() => {
    if (!isValid) {
      dispatch(refreshHotRanges(new HotRangesRequest()));
    }
  }, [dispatch, isValid]);
  useEffect(() => {
    async function updateNodeData() {
      setNodeData(await getNodeDatapoints(nodeIds));
      setMiscData(await getMiscData());
    }
    updateNodeData();
  }, [nodeIds]);

  let hottest: Datapoint = null;
  const ranges = hotRangesToDatapoints(hotRanges);
  const nodeRanges: Record<string, Datapoint[]> = {};
  for (const range of ranges) {
    if (hottest === null || range.qps > hottest.qps) {
      hottest = range;
    }
    if (!nodeRanges[range.node]) {
      nodeRanges[range.node] = [];
    }
    nodeRanges[range.node].push(range);
  }

  let results = miscData;
  if (hottest) {
    results = results.concat(collectResults(
      nodeData,
      nodeRanges[hottest.node],
      ranges,
    ));
  }

  return (
    <React.Fragment>
      <table>
        <tbody>
          {results.map((row: any, i: number) => {
            return (
              <tr key={i}>
                <td>{row.identifier}</td>
                <td>{row.value}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </React.Fragment>
  );
};

export default HotspotsExperiment;
