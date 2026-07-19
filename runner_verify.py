#!/usr/bin/env python3
# Runs ON a GitHub Actions runner (fresh Azure IP). Verifies a shard of Shopify candidate domains
# through the locked quality gate using the LIVE products.json only (country pre-supplied by pre-crawl).
#
# KEY: Shopify rate-limits per IP (~Retry-After 60s). We PACE requests with a global token bucket
# (RATE req/s, default 2) so we never blow the budget — concurrency exists only to overlap dead-host
# timeouts, NOT to burst. Anything still rate-limited (429/conn-reset) is retried after a 65s wait.
#
# Input:  stdin, "domain,country" per line   Output: keeper TSV domain\tcountry\tproducts\tscore\tphys\tbrand
import sys, os, json, ssl, time, threading, urllib.request, urllib.error, concurrent.futures as cf

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
WEST = {"US", "GB", "CA", "AU", "NZ", "IE"}
CTX = ssl.create_default_context(); CTX.check_hostname = False; CTX.verify_mode = ssl.CERT_NONE
RATE = float(os.environ.get("RATE", "2"))       # requests/sec per runner (per IP)
WORKERS = int(os.environ.get("WORKERS", "6"))   # concurrency only to overlap slow/dead hosts

class Bucket:
    def __init__(self, rate):
        self.rate = rate; self.tokens = rate; self.last = time.time(); self.lk = threading.Lock()
    def take(self):
        while True:
            with self.lk:
                now = time.time()
                self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate); self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1; return
            time.sleep(0.03)
BUCKET = Bucket(RATE)

def fetch(d):
    BUCKET.take()
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(f"https://{d}/products.json?limit=250", headers={"User-Agent": UA}),
            timeout=12, context=CTX)
        return ("ok", r.read(3_000_000)) if r.getcode() == 200 else ("dead", None)
    except urllib.error.HTTPError as e:
        return ("rate", None) if e.code in (429, 430, 529) else ("dead", None)
    except Exception:
        return "rate", None

def judge(d, country, body):
    try:
        prods = json.loads(body).get("products", [])
    except Exception:
        return None
    if len(prods) < 10:
        return None
    priced = phys = tv = 0
    for p in prods:
        for v in p.get("variants", []):
            tv += 1
            try:
                if float(v.get("price") or 0) > 0: priced += 1
            except (TypeError, ValueError): pass
            if v.get("requires_shipping"): phys += 1
    if priced < 3:
        return None
    n = len(prods)
    score = 40 + (25 if n >= 1 else 0) + (20 if n >= 10 else 0) + (15 if n >= 50 else 0)
    if score < 85:
        return None
    if country and country not in WEST:
        return None
    phys_frac = round(phys / tv, 2) if tv else 0.0
    brand = (str(prods[0].get("vendor") or d))[:60].replace("\t", " ").replace("\n", " ")
    return f"{d}\t{country or '?'}\t{n}\t{score}\t{phys_frac}\t{brand}"

def main():
    rows = []
    for l in sys.stdin:
        l = l.strip()
        if not l: continue
        p = l.split(",")
        rows.append((p[0].strip().lower(), (p[1].strip().upper() if len(p) > 1 else "")))
    hist = {"keeper": 0, "not_store": 0, "dead": 0, "rate_final": 0}
    t0 = time.time()

    def work(item):
        d, c = item
        st, body = fetch(d)
        if st == "ok":
            line = judge(d, c, body)
            return ("keeper", d, c, line) if line else ("not_store", d, c, None)
        return (st, d, c, None)

    pending = rows
    for rnd in range(4):                      # 1 paced pass + up to 3 retries (65s apart for the 60s window)
        retry = []
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for kind, d, c, line in ex.map(work, pending):
                if kind == "keeper":
                    print(line, flush=True); hist["keeper"] += 1
                elif kind == "not_store":
                    hist["not_store"] += 1
                elif kind == "dead":
                    hist["dead"] += 1
                else:
                    retry.append((d, c))
        if not retry:
            break
        sys.stderr.write(f"  round {rnd}: {len(retry)} rate-limited, waiting 65s\n"); sys.stderr.flush()
        time.sleep(65)
        pending = retry
    hist["rate_final"] = len(pending) if pending else 0
    dt = time.time() - t0
    done = hist["keeper"] + hist["not_store"] + hist["dead"]
    sys.stderr.write(f"[DIAG] {len(rows)} in {dt:.0f}s | {hist} | RATE={RATE} | ~{done/dt*60 if dt else 0:.0f}/min done\n")

if __name__ == "__main__":
    main()