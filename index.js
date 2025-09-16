import express from "express";
import axios from "axios";
import yauzl from "yauzl";
import { parse } from "csv-parse";

const app = express();
app.use(express.json({ limit: "10mb" }));

app.get("/health", (_req, res) => res.status(200).send("ok"));

/**
 * POST /unzip
 * body: { url, entryStart=0, rowOffset=0, rowLimit=5000 }
 * returns: { header: string[]|null, rows: string[][], nextEntry: number, nextOffset: number }
 */
app.post("/unzip", async (req, res) => {
  try {
    const { url, entryStart = 0, rowOffset = 0, rowLimit = 5000 } = req.body || {};
    if (!url) return res.status(400).json({ error: "url required" });

    // 1) Download the ZIP (ZIP64 supported later by yauzl)
    const zipResp = await axios.get(url, {
      responseType: "arraybuffer",
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://sco.ca.gov/upd_download_property_records.html",
        "Accept": "*/*"
      },
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      validateStatus: s => s < 500
    });
    if (zipResp.status >= 400) {
      return res.status(zipResp.status).json({ error: `fetch failed: ${zipResp.status}` });
    }
    const buffer = Buffer.from(zipResp.data);

    // 2) Unzip and stream-parse CSVs page-by-page
    const out = await unzipAndParsePage(buffer, {
      entryStart: Number(entryStart) || 0,
      rowOffset: Number(rowOffset) || 0,
      rowLimit: Math.min(Number(rowLimit) || 5000, 10000)
    });

    res.set("Content-Type", "application/json");
    res.status(200).send(JSON.stringify(out));
  } catch (e) {
    res.status(500).json({ error: e?.message || String(e) });
  }
});

function unzipAndParsePage(buffer, { entryStart, rowOffset, rowLimit }) {
  return new Promise((resolve, reject) => {
    let rows = [];
    let header = null;
    let entryIndex = -1;
    let consumed = 0;
    let resolved = false;

    yauzl.fromBuffer(buffer, { lazyEntries: true, autoClose: true }, (err, zip) => {
      if (err) return reject(err);
      zip.readEntry();

      zip.on("entry", (entry) => {
        entryIndex++;
        if (!/\.csv$/i.test(entry.fileName)) {
          return zip.readEntry();
        }
        if (entryIndex < entryStart) {
          return zip.readEntry();
        }
        zip.openReadStream(entry, (err2, rs) => {
          if (err2) return reject(err2);

          const parser = parse({ relax_quotes: true });
          let localHeader = null;
          let skip = (entryIndex === entryStart) ? rowOffset : 0;

          parser.on("readable", () => {
            let rec;
            while ((rec = parser.read()) !== null) {
              if (!localHeader) { localHeader = rec; continue; }
              if (skip > 0) { skip--; continue; }
              if (consumed < rowLimit) {
                rows.push(rec);
                consumed++;
              }
              if (consumed >= rowLimit && !resolved) {
                resolved = true;
                header = localHeader;
                try { rs.destroy(); } catch {}
                try { zip.close(); } catch {}
                return resolve({ header, rows, nextEntry: entryIndex, nextOffset: (rowOffset + rows.length) });
              }
            }
          });

          parser.on("end", () => {
            // finished this entry
            if (resolved) return;
            if (!header) header = localHeader;
            zip.readEntry();
          });

          parser.on("error", (e) => { if (!resolved) reject(e); });
          rs.on("error", (e) => { if (!resolved) reject(e); });
          rs.pipe(parser);
        });
      });

      zip.on("end", () => {
        if (!resolved) resolve({ header, rows, nextEntry: -1, nextOffset: -1 });
      });
      zip.on("error", (e) => { if (!resolved) reject(e); });
    });
  });
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`sco-unzip listening on ${PORT}`));
