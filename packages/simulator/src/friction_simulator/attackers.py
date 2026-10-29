"""The attacker models of the RBA papers, generated for chosen users. Simulated, labelled so.

Every attacker holds the victim's password. What differs is what else they know (Wiefling et al.,
ACM TOPS 2022, Section 5, p. 6:9; DECISIONS.md has each definition with its page):

- naive: half sign in from a random IP somewhere in the world with a random browser; half use the
  most common IP and the most common browser string among the attack data, as a resourced attacker
  would. IPs are drawn from the failed logins from known attack addresses.
- vpn: knows the victim's country and uses an address in it, drawn from failed attempts in that
  country, with one of the most popular browser strings of legitimate logins.
- targeted: knows the victim's location, device and browser: an address in the victim's country
  (the victim's region when the attack data has one), and a browser string whose browser, operating
  system and device type match the victim's usual ones.

The attack logins are scored against the victim's real history and never become part of it
(friction_features.logins: synthetic events never update the state).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from friction_simulator import draws

MODELS = ("naive", "vpn", "targeted")
REF_BASE = 10**12
POOL_COLUMNS = ["ip_key", "ip_prefix16", "asn", "country", "region", "city", "rtt_ms", "n"]
UA_COLUMNS = ["ua_key", "browser", "os", "device_type", "n"]


def _pick(frame: pd.DataFrame, u: float) -> pd.Series:
    return frame.iloc[min(int(u * len(frame)), len(frame) - 1)]


def generate(
    victims: pd.DataFrame,
    ip_pool: pd.DataFrame,
    attack_uas: pd.DataFrame,
    legit_uas: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    seed: int,
    purpose: str,
) -> pd.DataFrame:
    """victims: user_key, main_country, main_region, main_browser, main_os, main_device, main_ua_key.

    ip_pool: failed logins from attack addresses, one row per address with its count n.
    attack_uas and legit_uas: browser strings with their counts. Returns one row per victim and model.
    """
    victims = victims.sort_values("user_key", kind="mergesort").reset_index(drop=True)
    pool = ip_pool.sort_values(["n", "ip_key"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    by_country = {c: g.reset_index(drop=True) for c, g in pool.groupby("country", sort=True)}
    by_region = {k: g.reset_index(drop=True) for k, g in pool.groupby(["country", "region"], sort=True)}
    countries = sorted(by_country)
    a_uas = attack_uas.sort_values(["n", "ua_key"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    l_uas = legit_uas.sort_values(["n", "ua_key"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    top_legit = l_uas.head(5)
    keys = np.array([int(k[1:], 16) % (1 << 62) for k in victims["user_key"]], dtype=np.int64)
    span = (end - start).total_seconds()
    rows = []
    for m_index, model in enumerate(MODELS):
        u_time = draws.uniform(keys, seed, purpose, model, "time")
        u_a = draws.uniform(keys, seed, purpose, model, "a")
        u_b = draws.uniform(keys, seed, purpose, model, "b")
        u_c = draws.uniform(keys, seed, purpose, model, "c")
        for i, v in victims.iterrows():
            if model == "naive":
                if u_a[i] < 0.5:
                    country = countries[min(int(u_b[i] * len(countries)), len(countries) - 1)]
                    ip = _pick(by_country[country], u_c[i])
                    ua = _pick(l_uas, u_b[i])
                else:
                    ip, ua = pool.iloc[0], a_uas.iloc[0]
            elif model == "vpn":
                home = by_country.get(v["main_country"])
                ip = _pick(home if home is not None else pool, u_b[i])
                ua = _pick(top_legit, u_c[i])
            else:
                region = by_region.get((v["main_country"], v["main_region"]))
                home = by_country.get(v["main_country"])
                ip = _pick(region if region is not None else (home if home is not None else pool), u_b[i])
                same = a_uas[
                    (a_uas["browser"] == v["main_browser"])
                    & (a_uas["os"] == v["main_os"])
                    & (a_uas["device_type"] == v["main_device"])
                ]
                if same.empty:
                    same = l_uas[
                        (l_uas["browser"] == v["main_browser"])
                        & (l_uas["os"] == v["main_os"])
                        & (l_uas["device_type"] == v["main_device"])
                    ]
                ua = (
                    _pick(same, u_c[i])
                    if not same.empty
                    else pd.Series(
                        {
                            "ua_key": v["main_ua_key"],
                            "browser": v["main_browser"],
                            "os": v["main_os"],
                            "device_type": v["main_device"],
                        }
                    )
                )
            ts = start + pd.Timedelta(seconds=float(u_time[i]) * span)
            rows.append(
                {
                    "login_ref": REF_BASE + m_index * 10**9 + i,
                    "user_key": v["user_key"],
                    "event_ts": ts.floor("ms"),
                    "ip_key": ip["ip_key"],
                    "ip_prefix16": ip["ip_prefix16"],
                    "asn": int(ip["asn"]),
                    "country": ip["country"],
                    "region": ip["region"],
                    "city": ip["city"],
                    "rtt_ms": ip["rtt_ms"],
                    "ua_key": ua["ua_key"],
                    "browser": ua["browser"],
                    "os": ua["os"],
                    "device_type": ua["device_type"],
                    "success": True,
                    "synthetic": True,
                    "attacker_model": model,
                    "purpose": purpose,
                }
            )
    return pd.DataFrame(rows)
