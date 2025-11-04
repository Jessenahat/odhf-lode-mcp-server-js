import express from "express";
import cors from "cors";
import { readFile } from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { parse } from "csv-parse/sync";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// ---- CORS (public, no auth) ----
app.use(
  cors({
    origin: "*",
    credentials: false,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Accept"]
  })
);

app.use(express.json());

const CSV_FILE = path.join(__dirname, "odhf_v1.1.csv");

// ====== Lazy CSV loader (fast cold start) ======
let DATA = null;          // array of rows (objects)
let COLUMNS = null;       // list of column names

async function loadCSVOnce() {
  if (DATA) return;
  const raw = await readFile(CSV_FILE);
  // csv-parse/sync will auto-detect encodings if BOM; otherwise treat as UTF-8 bytes.
  // For cp1252 edge-cases, you can convert buffer to string via iconv-lite if needed.
  const text = raw.toString("utf8");
  const records = parse(text, {
    columns: true,
    skip_empty_lines: true,
    bom: true
  });
  DATA = records;
  COLUMNS = records.length ? Object.keys(records[0]) : [];
}

// ---- Small helpers ----
function findCol(cols, candidates) {
  const lower = new Map(cols.map((c) => [c.toLowerCase(), c]));
  for (const cand of candidates) {
    if (cols.includes(cand)) return cand;
    const m = lower.get(cand.toLowerCase());
    if (m) return m;
  }
  return null;
}

const ALIAS_MAP = {
  province: new Set([
    "province",
    "Province",
    "Province or Territory",
    "Province/Territory",
    "prov",
    "province_or_territory"
  ]),
  odhf_facility_type: new Set([
    "odhf_facility_type",
    "ODHF Facility Type",
    "Facility Type",
    "facility_type",
    "odhf facility type"
  ])
};

// ---- Health ----
app.get("/", async (_req, res) => {
  try {
    await loadCSVOnce();
    res.type("text/plain").send(
      `ODHF MCP Server (Node) is running! csv_found=true rows=${DATA?.length ?? 0}`
    );
  } catch (e) {
    res
      .status(500)
      .type("text/plain")
      .send(`Startup error: ${e?.message || String(e)}`);
  }
});

// ---- List fields ----
app.get("/list_fields", async (_req, res) => {
  try {
    await loadCSVOnce();
    if (!COLUMNS) {
      return res
        .status(400)
        .json({ error: `CSV not found or empty at ${CSV_FILE}` });
    }
    res.json({ columns: COLUMNS });
  } catch (e) {
    res.status(500).json({ error: e?.message || String(e) });
  }
});

// ---- Search ----
app.get("/search_facilities", async (req, res) => {
  try {
    await loadCSVOnce();
    if (!DATA || !COLUMNS)
      return res
        .status(400)
        .json({ error: `CSV not found or empty at ${CSV_FILE}` });

    const colProvince = findCol(COLUMNS, ALIAS_MAP.province);
    const colType = findCol(COLUMNS, ALIAS_MAP.odhf_facility_type);
    if (!colProvince || !colType) {
      return res.status(400).json({
        error: "Expected columns not found.",
        have: COLUMNS,
        need_any_of: {
          province: Array.from(ALIAS_MAP.province),
          odhf_facility_type: Array.from(ALIAS_MAP.odhf_facility_type)
        }
      });
    }

    const province = (req.query.province || "").toString().trim();
    const facilityType = (req.query.facility_type || "").toString().trim();

    let rows = DATA;
    if (province) {
      const p = province.toLowerCase();
      rows = rows.filter((r) => (r[colProvince] || "").toString().toLowerCase().includes(p));
    }
    if (facilityType) {
      const f = facilityType.toLowerCase();
      rows = rows.filter((r) => (r[colType] || "").toString().toLowerCase().includes(f));
    }

    if (rows.length === 0) {
      return res.json({
        message:
          "No results. Try another province (e.g., 'QC'/'Quebec') or facility_type."
      });
    }

    const preferred = [
      "Facility Name",
      "City",
      colProvince,
      colType,
      "Postal Code",
      "Latitude",
      "Longitude"
    ].filter((c) => COLUMNS.includes(c));

    const cleaned = rows.slice(0, 25).map((r) => {
      const obj = {};
      if (preferred.length) {
        for (const c of preferred) obj[c] = nullish(r[c]);
      } else {
        for (const c of COLUMNS) obj[c] = nullish(r[c]);
      }
      return obj;
    });

    res.json(cleaned);
  } catch (e) {
    res.status(500).json({ error: e?.message || String(e) });
  }
});

function nullish(v) {
  // normalize NaN/undefined/empty to null where sensible
  if (v === undefined || v === "") return null;
  if (v === "NaN" || v === "nan") return null;
  return v;
}

// ---- MCP manifest ----
const TOOLS_MANIFEST = [
  {
    name: "list_fields",
    description: "List dataset columns",
    input_schema: { type: "object", properties: {} }
  },
  {
    name: "search_facilities",
    description: "Search facilities by province and/or ODHF facility type",
    input_schema: {
      type: "object",
      properties: {
        province: { type: "string" },
        facility_type: { type: "string" }
      }
    }
  }
];

// ---- SSE helpers ----
function sseHeaders(res) {
  res.set({
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
    "Access-Control-Allow-Origin": "*",
    "Content-Type": "text/event-stream; charset=utf-8"
  });
}

function sseWrite(res, event, dataObj) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(dataObj)}\n\n`);
}

// One-shot: send list_tools then close (best for ChatGPT connector)
app.get("/sse_once", async (_req, res) => {
  try {
    sseHeaders(res);
    // respond immediately; no need to load CSV for tool discovery
    const payload = { event: "list_tools", data: { tools: TOOLS_MANIFEST } };
    sseWrite(res, "message", payload);
    // small delay to flush across proxies, then end
    setTimeout(() => res.end(), 50);
  } catch (e) {
    res.status(500).end();
  }
});

// Debug SSE with keepalive pings
app.get("/sse", async (req, res) => {
  sseHeaders(res);
  sseWrite(res, "message", { event: "list_tools", data: { tools: TOOLS_MANIFEST } });

  const keep = setInterval(() => {
    if (res.writableEnded) return clearInterval(keep);
    sseWrite(res, "ping", "keepalive");
  }, 10000);

  req.on("close", () => clearInterval(keep));
});

// ---- Start server ----
const PORT = process.env.PORT || 8080; // Railway/Render expect 8080
app.listen(PORT, () => {
  console.log(`ODHF MCP (Node) listening on ${PORT}`);
});
