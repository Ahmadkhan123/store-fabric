#!/usr/bin/env python3
# Runs ON a GitHub Actions runner (fresh Azure IP). Verifies a shard of Shopify candidate domains
# through the locked quality gate using the LIVE products.json only (country is pre-supplied by the
# pre-crawl, so no homepage fetch needed). stdlib-only (no pip install → fast cold start).
#
# Input:  stdin, one "domain,country" per line (country optional; already English-filtered upstream)
# Output: stdout, keeper TSV: domain \t country \t products \t score \t phys_frac \t brand
# Diag:   stderr histogram (keeper / not-store / rate-limited / dead) + sustained rate
#
# Burst-then-retry: fire the shard concurrently (fresh IP bucket absorbs it); anything that looks
# rate-limited (HTTP 429 or connection 000) is RETRIED paced — never counted as a reject.
import sys, os, json, ssl, time, urllib.request, urllib.error, concurrent.futures as cf

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
WEST = {"US", "GB", "CA", "AU", "NZ", "IE"}
CTX = ssl.create_default_context(); CTX.check_hostname = False; CTX.verify_mode = ssl.CERT_NONE

def fetch(d):
    """returns ('ok', body) | ('rate', None) | ('dead', None)"""
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(f"https://{d}/products.json?limit=250", headers={"User-Agent": UA}),
            timeout=12, context=CTX)
        if r.getcode() == 200:
            return "ok", r.read(3_000_000)
        return "dead", None
    except urllib.error.HTTPError as e:
        return ("rate", None) if e.code in (429, 430, 529) else ("dead", None)
    except Exception:
        return "rate", None      # connection reset / timeout / 000 → retryable, not a reject

def judge(d, country, body):
    """apply the locked gate to a products.json body; return keeper line or None"""
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
    if priced < 3:                          # all-$0 template/placeholder guard
        return None
    n = len(prods)
    score = 40 + (25 if n >= 1 else 0) + (20 if n >= 10 else 0) + (15 if n >= 50 else 0)
    if score < 85:
        return None
    if country and country not in WEST:     # english-first (country pre-supplied)
        return None
    phys_frac = round(phys / tv, 2) if tv else 0.0
    brand = (str(prods[0].get("vendor") or d))[:60].replace("\t", " ").replace("\n", " ")
    return f"{d}\t{country or '?'}\t{n}\t{score}\t{phys_frac}\t{brand}"

def main():
    rows = []
    for l in sys.stdin:
        l = l.strip()
        if not l: continue
        parts = l.split(",")
        rows.append((parts[0].strip().lower(), (parts[1].strip().upper() if len(parts) > 1 else "")))
    hist = {"keeper": 0, "not_store": 0, "dead": 0, "rate_final": 0}
    t0 = time.time()

    def work(item):
        d, c = item
        st, body = fetch(d)
        if st == "ok":
            line = judge(d, c, body)
            return ("keeper", d, c, line) if line else ("not_store", d, c, None)
        return (st, d, c, None)   # 'rate' or 'dead'

    pending = rows
    for rnd in range(4):                     # 1 burst + up to 3 paced retry rounds
        retry = []
        workers = 16 if rnd == 0 else 5      # burst first (fresh Azure IP, no rate limit), then gentle
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for kind, d, c, line in ex.map(work, pending):
                if kind == "keeper":
                    print(line, flush=True); hist["keeper"] += 1
                elif kind == "not_store":
                    hist["not_store"] += 1
                elif kind == "dead":
                    hist["dead"] += 1
                else:                        # rate → retry next round
                    retry.append((d, c))
        if not retry:
            break
        sys.stderr.write(f"  round {rnd}: {len(retry)} rate-limited, pacing 20s then retry\n"); sys.stderr.flush()
        time.sleep(20)
        pending = retry
    hist["rate_final"] = len(pending) if 'retry' in dir() else 0
    for d, c in (pending if pending else []):
        pass
    dt = time.time() - t0
    done = hist["keeper"] + hist["not_store"] + hist["dead"]
    rate = (done / dt * 60) if dt else 0
    sys.stderr.write(f"[DIAG] {len(rows)} domains in {dt:.0f}s | {hist} | ~{rate:.0f}/min sustained\n")

if __name__ == "__main__":
    main()