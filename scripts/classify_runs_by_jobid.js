// classify_runs_by_jobid.js
//
// The `jobs` collection TTL-deletes documents once a job's end_date passes, so it
// only ever shows currently-live jobs - it CANNOT reconstruct full run history.
// `runs` has no TTL and holds the complete history, but doesn't store the docker
// image. This script works around that by classifying each run's measurement
// type from keywords in its `jobid` (which researchers name descriptively, e.g.
// "curl-ip-w", "iperf-downlink-sat1") - full-history coverage, no image needed.
//
// Where a `jobs` document for that jobid still happens to exist (not yet TTL'd),
// it also extracts the real docker image and reports it alongside the keyword
// guess, as a sanity check on the classification rules below.
//
// Load as a file (do NOT pipe via stdin):
//   docker cp scripts/classify_runs_by_jobid.js orchestrator-datastore-1:/tmp/classify_runs_by_jobid.js
//   docker exec -i orchestrator-datastore-1 mongosh leotest --quiet --file /tmp/classify_runs_by_jobid.js
//
// Read-only: only find/aggregate, never writes to the database.

const sixMonthsAgo = new Date();
sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

// Checked in order, first match wins. Add more rules as real jobid patterns show up.
const CATEGORY_RULES = [
  [/iperf/i, "Throughput (iperf)"],
  [/speedtest|ookla/i, "Speed test (Ookla/Speedtest)"],
  [/(^|[^a-z])mtr([^a-z]|$)|traceroute|tracert|trace-route/i, "Path / traceroute (mtr)"],
  [/(^|[^a-z])ping([^a-z]|$)|latency|rtt/i, "Latency (ping)"],
  [/dash|video|stream/i, "Video streaming (DASH)"],
  [/grpc|starlink/i, "gRPC / Starlink telemetry"],
  [/dns/i, "DNS lookup"],
  [/curl|http|download|upload|fetch|web/i, "HTTP transfer (curl/http)"],
  [/hello-world|^test|sample|demo/i, "Test/sample job"]
];

function classify(jobid) {
  const id = jobid || "";
  for (const [re, label] of CATEGORY_RULES) {
    if (re.test(id)) return label;
  }
  return "Unclassified (needs manual look / image lookup)";
}

function extractImage(job) {
  if (!job) return null;
  if (job.params && typeof job.params.execute === "string") {
    const m = job.params.execute.match(/image=([^;]+)/);
    if (m) return m[1].trim();
  }
  if (typeof job.config === "string") {
    const m = job.config.match(/docker:\s*\n\s*image:\s*["']?([^\n"']+)/);
    if (m) return m[1].trim();
  }
  return null;
}

print("============================================================");
print("Run classification by jobid keywords (full history, no TTL issue)");
print("============================================================");

const runs = db.runs
  .aggregate([
    { $lookup: { from: "jobs", localField: "jobid", foreignField: "id", as: "job" } },
    { $addFields: { job: { $arrayElemAt: ["$job", 0] } } }
  ])
  .toArray();

print("Total runs: " + runs.length + "\n");

const byCategory = {};
runs.forEach((r) => {
  const category = classify(r.jobid);
  if (!byCategory[category]) {
    byCategory[category] = { total: 0, last6mo: 0, users: new Set(), jobids: new Set() };
  }
  const c = byCategory[category];
  c.total += 1;
  if (r.start_time && r.start_time >= sixMonthsAgo) c.last6mo += 1;
  c.users.add(r.userid);
  c.jobids.add(r.jobid);
});

print("--- By measurement category ---");
Object.entries(byCategory)
  .sort((a, b) => b[1].total - a[1].total)
  .forEach(([category, c]) => {
    print("\n" + category);
    print("  runs: total=" + c.total + " past_6mo=" + c.last6mo);
    print("  distinct users: " + c.users.size);
    print("  distinct jobids (" + c.jobids.size + "): " + Array.from(c.jobids).slice(0, 15).join(", ") + (c.jobids.size > 15 ? ", ..." : ""));
  });

print("\n============================================================");
print("Distinct jobids, run counts, and image (where the job doc still exists)");
print("============================================================");

const byJobid = {};
runs.forEach((r) => {
  if (!byJobid[r.jobid]) {
    byJobid[r.jobid] = { total: 0, last6mo: 0, users: new Set(), image: extractImage(r.job) };
  }
  const j = byJobid[r.jobid];
  j.total += 1;
  if (r.start_time && r.start_time >= sixMonthsAgo) j.last6mo += 1;
  j.users.add(r.userid);
});

Object.entries(byJobid)
  .sort((a, b) => b[1].total - a[1].total)
  .forEach(([jobid, j]) => {
    print(
      jobid.padEnd(40) +
        " category=" + classify(jobid).padEnd(35) +
        " runs=" + String(j.total).padEnd(6) +
        " 6mo=" + String(j.last6mo).padEnd(5) +
        " users=" + String(j.users.size).padEnd(4) +
        " image=" + (j.image || "(job expired - unknown)")
    );
  });

print("\nDone");
