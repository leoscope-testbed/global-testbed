// analyze_testbed_usage.js
//
// Read-only usage report for the LEOScope testbed's Mongo database (`leotest`).
// Answers:
//   1. Who is using the testbed (total, and in the past 6 months), by email domain.
//   2. What docker images / measurement types they run (total, and in the past 6 months).
//
// Run against the production `datastore` mongo container. Do NOT pipe this file in via
// stdin (`mongosh ... < script.js`) - mongosh treats stdin as interactive REPL input fed
// line-by-line, which breaks on any multi-line statement (aggregate pipelines, etc).
// Instead, copy it in and load it as a file:
//
//   docker cp scripts/analyze_testbed_usage.js datastore:/tmp/analyze_testbed_usage.js
//   docker exec -i datastore mongosh leotest --quiet --file /tmp/analyze_testbed_usage.js
//
// or, if the db name differs:
//   docker exec -i datastore mongosh "mongodb://127.0.0.1:27017/<DB_NAME>" --quiet --file /tmp/analyze_testbed_usage.js
//
// This script only reads (find/aggregate/distinct) - it never writes to the database.
//
// IMPORTANT CAVEAT (read this before trusting the "measurement type" numbers):
// The `jobs` collection has a TTL index on `expire_at` (= the job's end_date). Once a
// job's end_date passes, MongoDB automatically deletes the whole document - including
// the docker image info stored in `config`/`params`. So `jobs` only ever reflects
// jobs that are still active or scheduled for the future, never a full history.
// The `runs` collection (one doc per actual execution) has NO such TTL and keeps full
// history of who ran what and when - but it does not store the docker image itself,
// only jobid/nodeid/userid/timestamps. So per-image counts below are a *best-effort*
// join of runs -> still-existing jobs; runs whose job has already expired show up as
// "(job expired/deleted)". If you need a fully accurate historical image breakdown,
// image info needs to start being copied onto the `runs` document at execution time,
// or into a non-expiring collection - happy to wire that up if useful.

const sixMonthsAgo = new Date();
sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

function hr(title) {
  print("\n============================================================");
  print(title);
  print("============================================================");
}

function padEnd(s, n) {
  s = String(s);
  return s.length >= n ? s : s + " ".repeat(n - s.length);
}

function domainOf(id) {
  id = String(id || "");
  const at = id.lastIndexOf("@");
  return at >= 0 ? id.slice(at + 1).toLowerCase() : "(no-email-id)";
}

function extractImage(job) {
  if (!job) return "(job expired/deleted)";
  // Legacy convention: params.execute = "image=<name>" (rarely used by real jobs -
  // the website always schedules with params.execute = "-").
  if (job.params && typeof job.params.execute === "string") {
    const m = job.params.execute.match(/image=([^;]+)/);
    if (m) return m[1].trim();
  }
  // Real source of truth: the uploaded experiment-config.yaml stored verbatim in
  // job.config, which always starts with a `docker:` block whose second line is
  // `  image: <value>` (see ScheduleExperiments.js configYaml builder).
  if (typeof job.config === "string") {
    const m = job.config.match(/docker:\s*\n\s*image:\s*["']?([^\n"']+)/);
    if (m) return m[1].trim();
  }
  return "(no image field found)";
}

// ---------------------------------------------------------------------------
// 1. Registered users
// ---------------------------------------------------------------------------
hr("1. Registered user accounts");

const humanRoleFilter = { role: { $nin: [2, 4] } }; // exclude NODE(2), NODE_PRIV(4)
const hasEmailId = { id: { $regex: /@/ } };
const isActiveAccount = {
  registration_status: { $nin: ["pending_signup", "signup_link_sent"] },
  $or: [
    { access_token: { $exists: true, $ne: "" } },
    { static_access_token: { $exists: true, $ne: "" } }
  ]
};

const totalAccountsAllStates = db.users.countDocuments(Object.assign({}, humanRoleFilter, hasEmailId));
const totalActiveAccounts = db.users.countDocuments(Object.assign({}, humanRoleFilter, hasEmailId, isActiveAccount));
const totalPending = totalAccountsAllStates - totalActiveAccounts;

print("Human user docs (id looks like an email, role is not NODE/NODE_PRIV): " + totalAccountsAllStates);
print("  of which active (completed signup, has an access token): " + totalActiveAccounts);
print("  of which still pending signup / access request:          " + totalPending);

// ---------------------------------------------------------------------------
// 2. Who actually used the testbed (from `runs`, which has no TTL / full history)
// ---------------------------------------------------------------------------
hr("2. Users who actually ran something (from `runs` collection, full history)");

const runUsersAll = db.runs.distinct("userid");
const runUsers6mo = db.runs.distinct("userid", { start_time: { $gte: sixMonthsAgo } });

print("Distinct users with >=1 run, ALL TIME:      " + runUsersAll.length);
print("Distinct users with >=1 run, PAST 6 MONTHS: " + runUsers6mo.length);
print("(six-month cutoff used: " + sixMonthsAgo.toISOString() + ")");

// ---------------------------------------------------------------------------
// 3. Email domain breakdown
// ---------------------------------------------------------------------------
function printDomainBreakdown(title, ids) {
  print("\n--- " + title + " ---");
  const counts = {};
  ids.forEach((id) => {
    const d = domainOf(id);
    counts[d] = (counts[d] || 0) + 1;
  });
  Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([domain, count]) => print("  " + padEnd(domain, 40) + count));
}

hr("3. Email domain breakdown");

printDomainBreakdown(
  "All registered human users (" + totalAccountsAllStates + ")",
  db.users.find(Object.assign({}, humanRoleFilter, hasEmailId), { id: 1 }).toArray().map((u) => u.id)
);

printDomainBreakdown("Users with a run, ALL TIME (" + runUsersAll.length + ")", runUsersAll);

printDomainBreakdown("Users with a run, PAST 6 MONTHS (" + runUsers6mo.length + ")", runUsers6mo);

// ---------------------------------------------------------------------------
// 4. Docker images currently scheduled (live snapshot, see TTL caveat above)
// ---------------------------------------------------------------------------
hr("4. Docker images in the LIVE `jobs` collection (snapshot, not full history)");

const liveJobs = db.jobs.find({}, { id: 1, userid: 1, config: 1, params: 1 }).toArray();
const liveImageCounts = {};
const liveImageUsers = {};
liveJobs.forEach((job) => {
  const image = extractImage(job);
  liveImageCounts[image] = (liveImageCounts[image] || 0) + 1;
  if (!liveImageUsers[image]) liveImageUsers[image] = new Set();
  liveImageUsers[image].add(job.userid);
});

print("Total non-expired job documents right now: " + liveJobs.length + "\n");
Object.entries(liveImageCounts)
  .sort((a, b) => b[1] - a[1])
  .forEach(([image, count]) =>
    print("  " + padEnd(image, 40) + "jobs=" + padEnd(count, 6) + "distinct_users=" + liveImageUsers[image].size)
  );

// ---------------------------------------------------------------------------
// 5. Measurement types by RUN count (best-effort join runs -> still-existing jobs)
// ---------------------------------------------------------------------------
function runImageReport(title, matchStage) {
  print("\n--- " + title + " ---");
  const pipeline = [];
  if (matchStage) pipeline.push({ $match: matchStage });
  pipeline.push(
    { $lookup: { from: "jobs", localField: "jobid", foreignField: "id", as: "job" } },
    { $addFields: { job: { $arrayElemAt: ["$job", 0] } } }
  );
  const runs = db.runs.aggregate(pipeline).toArray();
  const counts = {};
  runs.forEach((r) => {
    const image = extractImage(r.job);
    counts[image] = (counts[image] || 0) + 1;
  });
  print("  total runs in this window: " + runs.length);
  Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([image, count]) => print("  " + padEnd(image, 40) + "runs=" + count));
}

hr("5. Measurement types by RUN count (best-effort - see TTL caveat above)");
runImageReport("All time", null);
runImageReport("Past 6 months", { start_time: { $gte: sixMonthsAgo } });

// ---------------------------------------------------------------------------
// 6. Per-user detail table
// ---------------------------------------------------------------------------
hr("6. Per-user detail (domain, run counts, images seen)");

const allRunsWithJobs = db.runs
  .aggregate([
    { $lookup: { from: "jobs", localField: "jobid", foreignField: "id", as: "job" } },
    { $addFields: { job: { $arrayElemAt: ["$job", 0] } } }
  ])
  .toArray();

const perUser = {};
allRunsWithJobs.forEach((r) => {
  const uid = r.userid || "(unknown)";
  if (!perUser[uid]) perUser[uid] = { total: 0, last6mo: 0, images: new Set() };
  perUser[uid].total += 1;
  if (r.start_time && r.start_time >= sixMonthsAgo) perUser[uid].last6mo += 1;
  perUser[uid].images.add(extractImage(r.job));
});

Object.entries(perUser)
  .sort((a, b) => b[1].total - a[1].total)
  .forEach(([uid, stats]) => {
    print(
      "  " +
        padEnd(uid, 40) +
        "domain=" +
        padEnd(domainOf(uid), 25) +
        "total_runs=" +
        padEnd(stats.total, 8) +
        "runs_6mo=" +
        padEnd(stats.last6mo, 8) +
        "images=[" +
        Array.from(stats.images).join(", ") +
        "]"
    );
  });

hr("Done");
