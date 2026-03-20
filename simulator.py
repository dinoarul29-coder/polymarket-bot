def _normalize_candidate(mkt: dict, source: str = "") -> Optional[dict]:
    question = str(mkt.get("question") or mkt.get("title") or "")
    desc = str(mkt.get("description") or "")
    slug = str(mkt.get("slug") or "")
    group_title = str(mkt.get("groupItemTitle") or "")
    combined = f"{question} {desc} {slug} {group_title}".lower()

    slug_match = "btc-updown-5m" in slug
    is_btc = bool(_BTC_RE.search(combined))
    is_5m = bool(_FIVEMIN_RE.search(combined)) or slug_match
    is_updown = _has_updown(combined) or slug_match

    if not is_btc or not is_5m or not is_updown:
        return None
    if mkt.get("closed") or mkt.get("archived") or mkt.get("resolved"):
        return None
    if mkt.get("enableOrderBook") is False:
        return None

    expiry = _parse_expiry(mkt)
    now = time.time()
    if expiry is None or expiry <= now:
        return None

    token_info = _extract_tokens(mkt)
    if token_info is None:
        return None

    up_label, dn_label, up_tid, dn_tid = token_info

    fee_rate = 0.0
    for key in ("feeRate", "makerBaseFee", "takerBaseFee", "fee_rate"):
        val = mkt.get(key)
        if val is not None:
            try:
                fee_rate = float(val)
                if fee_rate > 0:
                    break
            except (TypeError, ValueError):
                pass

    return {
        "market_id":   str(mkt.get("id") or mkt.get("conditionId") or slug or "?"),
        "question":    question or slug or "unknown",
        "expiry_ts":   expiry,
        "up_label":    up_label,
        "dn_label":    dn_label,
        "token_id_up": up_tid,
        "token_id_dn": dn_tid,
        "fee_rate":    fee_rate,
        "slug":        slug,
        "source":      source,
    }
