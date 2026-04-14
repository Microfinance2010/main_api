import pandas as pd
import json
import os


import time
import requests
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
from dataclasses import asdict

SEC_HEADERS = {
    # bitte anpassen: Name + Email/URL
    "User-Agent": "Your Name your.email@example.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}


def _resolve_company_tickers_path() -> str:
    package_dir = os.path.dirname(__file__)
    candidates = [
        os.path.join(package_dir, "assets", "company_tickers.json"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(package_dir))), "assets", "company_tickers.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError("company_tickers.json wurde weder im Paket noch im Repo gefunden")

def cik10(cik: int | str) -> str:
    return str(cik).lstrip("0").zfill(10)

def sec_get_json(url: str) -> dict:
    # einfache Fair-Access Bremse (konservativ)
    time.sleep(0.15)
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def pick_fact(companyfacts: dict, tag: str, unit_hint: str | None, fy: int, form_filter: tuple = ("10-K", "10-K/A")) -> float | None:
    """
    Holt einen Fact-Wert aus companyfacts für ein bestimmtes Fiscal Year.
    
    Args:
        companyfacts: SEC API companyfacts JSON
        tag: XBRL Tag Name (z.B. "NetIncomeLoss")
        unit_hint: Gewünschte Unit (z.B. "USD", "shares")
        fy: Fiscal Year
        form_filter: Tuple mit erlaubten Form-Typen (default: 10-K/10-K/A)
    
    Returns:
        Float-Wert oder None
    """
    facts = companyfacts.get("facts", {}).get("us-gaap", {}).get(tag, {})
    units = facts.get("units", {})
    if not units:
        return None

    # wenn unit_hint nicht passt/None ist, nimm "irgendeine" unit (typisch USD oder USD/shares)
    unit_keys = [unit_hint] if unit_hint in units else list(units.keys())

    candidates = []
    for unit in unit_keys:
        for item in units.get(unit, []):
            if item.get("fy") != fy:
                continue
            if item.get("fp") != "FY":
                continue
            if item.get("form") not in form_filter:
                continue
            if "val" in item and item["val"] is not None:
                candidates.append(item)

    if not candidates:
        return None

    # Robuste Auswahl:
    # 1) bevorzuge period end im Zieljahr (end[:4] == fy)
    # 2) bevorzuge plausible FY-Dauer (ca. 330-370 Tage)
    # 3) dann zuletzt eingereicht / spätestes period end
    def _end_year(item: dict) -> int | None:
        end = item.get("end")
        if not end or len(end) < 4:
            return None
        try:
            return int(end[:4])
        except Exception:
            return None

    def _is_full_year_duration(item: dict) -> bool:
        start = item.get("start")
        end = item.get("end")
        if not start or not end:
            return False
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            return False
        days = (end_dt - start_dt).days
        return 330 <= days <= 370

    candidates.sort(
        key=lambda x: (
            _end_year(x) == fy,
            _is_full_year_duration(x),
            x.get("filed", ""),
            x.get("end", ""),
        ),
        reverse=True,
    )
    return float(candidates[0]["val"])


def get_latest_quarterly_data(cik: int | str, fy: int) -> dict:
    """
    Holt die neuesten Quartalsdaten (10-Q) nach dem letzten 10-K Filing.
    Berechnet YTD-Veränderungen für Net Income, Operating CF und CAPEX.
    
    Args:
        cik: SEC CIK Nummer
        fy: Fiscal Year des letzten 10-K
    
    Returns:
        Dictionary mit quarterly_net_income, quarterly_ocf, quarterly_capex,
        ytd_quarters (Anzahl Quartale seit 10-K), projected_annual_change
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10(cik)}.json"
    cf = sec_get_json(url)
    
    # Finde das letzte 10-K Filing für fy
    def get_10k_filing_date(tag: str, unit: str, fy_year: int):
        """Findet das Filing-Datum des letzten 10-K für ein bestimmtes FY"""
        facts = cf.get("facts", {}).get("us-gaap", {}).get(tag, {})
        units = facts.get("units", {})
        if unit not in units:
            return None

        candidates = [
            item for item in units[unit]
            if item.get("fy") == fy_year and item.get("fp") == "FY" and item.get("form") in ("10-K", "10-K/A")
        ]
        if not candidates:
            return None

        candidates.sort(
            key=lambda x: (
                str(x.get("end", "")).startswith(str(fy_year)),
                x.get("filed", ""),
                x.get("end", ""),
            ),
            reverse=True,
        )
        return candidates[0].get("filed")
        return None
    
    # Hole 10-K Filing-Datum (verwende NetIncomeLoss als Referenz)
    filing_date_10k = get_10k_filing_date("NetIncomeLoss", "USD", fy)
    
    if not filing_date_10k:
        return {
            "has_quarterly_data": False,
            "quarterly_net_income_ytd": None,
            "quarterly_ocf_ytd": None,
            "quarterly_capex_ytd": None,
            "quarterly_fcf_ytd": None,
            "ytd_quarters": 0,
            "projected_annual_ni_change": 0,
            "projected_annual_ocf_change": 0,
            "projected_annual_capex_change": 0,
            "quarterly_period_start": None,
            "quarterly_period_end": None,
            "quarterly_period_year": None,
            "quarterly_filed_date": None,
        }
    
    def _is_ytd_duration(item: dict) -> bool:
        """
        Validiert, dass ein 10-Q Fact eine YTD-Periode repräsentiert (nicht nur Einzelquartal).

        SEC-Facts enthalten typischerweise start/end für duration tags. Für Q1/Q2/Q3
        erwarten wir ungefähr 3/6/9 Monate Periodenlänge.
        """
        fp = item.get("fp", "")
        if fp not in {"Q1", "Q2", "Q3"}:
            return False

        start = item.get("start")
        end = item.get("end")
        if not start or not end:
            return False

        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            return False

        days = (end_dt - start_dt).days
        expected_ranges = {
            "Q1": (60, 120),
            "Q2": (130, 220),
            "Q3": (220, 320),
        }
        low, high = expected_ranges[fp]
        return low <= days <= high

    # Finde alle 10-Q Filings NACH dem 10-K Filing für das nächste FY
    def get_quarterly_ytd_value(tag: str, unit: str, next_fy: int, after_date: str):
        """Holt den neuesten YTD-Wert aus 10-Q Filings nach einem bestimmten Datum"""
        facts = cf.get("facts", {}).get("us-gaap", {}).get(tag, {})
        units = facts.get("units", {})
        if unit not in units:
            return None, 0, None
        
        quarterly_items = []
        for item in units[unit]:
            # Suche nach 10-Q Filings im nächsten FY, die nach dem 10-K kamen
            if (item.get("fy") == next_fy and 
                item.get("form") == "10-Q" and 
                item.get("filed", "") > after_date):
                if _is_ytd_duration(item):
                    quarterly_items.append(item)
        
        if not quarterly_items:
            return None, 0, None
        
        # Sortiere robust: neuestes Filing zuerst, innerhalb desselben Filings
        # den aktuellsten Perioden-Endwert (statt Vorjahresvergleich) bevorzugen.
        quarterly_items.sort(
            key=lambda x: (
                x.get("filed", ""),
                x.get("end", ""),
                x.get("start", ""),
            ),
            reverse=True,
        )
        latest = quarterly_items[0]
        
        # Bestimme Anzahl Quartale basierend auf fp (Q1, Q2, Q3)
        fp = latest.get("fp", "")
        quarters_map = {"Q1": 1, "Q2": 2, "Q3": 3}
        quarters = quarters_map.get(fp, 0)
        
        return latest.get("val"), quarters, latest
    
    next_fy = fy + 1
    
    # Hole YTD-Werte für die drei Metriken
    ni_ytd, ni_quarters, ni_item = get_quarterly_ytd_value("NetIncomeLoss", "USD", next_fy, filing_date_10k)
    ocf_ytd, ocf_quarters, ocf_item = get_quarterly_ytd_value("NetCashProvidedByUsedInOperatingActivities", "USD", next_fy, filing_date_10k)
    
    # CAPEX: probiere verschiedene Tags
    capex_ytd, capex_quarters, capex_item = None, 0, None
    capex_tags = [
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PurchasesOfPropertyPlantAndEquipment"
    ]
    for capex_tag in capex_tags:
        capex_ytd, capex_quarters, capex_item = get_quarterly_ytd_value(capex_tag, "USD", next_fy, filing_date_10k)
        if capex_ytd is not None:
            break
    
    # Verwende die Quartalsanzahl der verfügbaren Daten
    quarters = max(ni_quarters, ocf_quarters, capex_quarters)
    
    # Extrapoliere auf Gesamtjahr (4 Quartale)
    def extrapolate_annual(ytd_value, quarters_count):
        """Extrapoliert YTD-Wert auf Gesamtjahr"""
        if ytd_value is None or quarters_count == 0:
            return 0
        return (ytd_value / quarters_count) * 4
    
    projected_ni = extrapolate_annual(ni_ytd, quarters)
    projected_ocf = extrapolate_annual(ocf_ytd, quarters)
    projected_capex = extrapolate_annual(capex_ytd, quarters)
    quarterly_fcf_ytd = (ocf_ytd - abs(capex_ytd)) if (ocf_ytd is not None and capex_ytd is not None) else None

    # Referenz-Periode für LLM/GUI: bevorzuge die Metrik mit größter Quartalsabdeckung.
    # Bei Gleichstand: CAPEX > OCF > Net Income (für CAPEX-lastige Fragen hilfreich).
    period_candidates = [
        (capex_quarters, capex_item, 3),
        (ocf_quarters, ocf_item, 2),
        (ni_quarters, ni_item, 1),
    ]
    period_candidates.sort(key=lambda x: (x[0], x[2]), reverse=True)
    selected_period_item = period_candidates[0][1] if period_candidates else None

    quarterly_period_start = selected_period_item.get("start") if selected_period_item else None
    quarterly_period_end = selected_period_item.get("end") if selected_period_item else None
    quarterly_filed_date = selected_period_item.get("filed") if selected_period_item else None
    quarterly_period_year = None
    if quarterly_period_end and len(quarterly_period_end) >= 4:
        try:
            quarterly_period_year = int(quarterly_period_end[:4])
        except Exception:
            quarterly_period_year = None
    
    return {
        "has_quarterly_data": quarters > 0,
        "quarterly_net_income_ytd": ni_ytd,
        "quarterly_ocf_ytd": ocf_ytd,
        "quarterly_capex_ytd": capex_ytd,
        "quarterly_fcf_ytd": quarterly_fcf_ytd,
        "ytd_quarters": quarters,
        "projected_annual_ni": projected_ni,
        "projected_annual_ocf": projected_ocf,
        "projected_annual_capex": abs(projected_capex) if projected_capex else 0,  # Normalize to positive
        "filing_date_reference": filing_date_10k,
        "quarterly_period_start": quarterly_period_start,
        "quarterly_period_end": quarterly_period_end,
        "quarterly_period_year": quarterly_period_year,
        "quarterly_filed_date": quarterly_filed_date,
    }

def get_core_metrics_from_companyfacts(cik: int | str, fy: int) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10(cik)}.json"
    cf = sec_get_json(url)
    # Tag-Fallbacks (besonders für Dividenden/Equity/Depreciation sinnvoll)
    tag_map = {
        "net_income": [("NetIncomeLoss", "USD")],
        "depreciation": [
            ("DepreciationDepletionAndAmortization", "USD"),
            ("DepreciationAndAmortization", "USD"),
        ],
        "equity": [
            ("StockholdersEquity", "USD"),
            ("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "USD"),
        ],
        "total_liabilities": [
            ("Liabilities", "USD"),
            ("LiabilitiesCurrent", "USD"),  # Fallback wenn nur Current verfügbar
        ],
        "dividends": [
            ("PaymentsOfDividends", "USD"),
            ("PaymentsOfDividendsCommonStock", "USD"),
            ("DividendsCommonStockCash", "USD"),
        ],
        "diluted_eps": [("EarningsPerShareDiluted", "USD/shares")],
        "shares_outstanding": [
            ("WeightedAverageNumberOfDilutedSharesOutstanding", "shares"),  # Verwässert (konsistent mit diluted_eps)
            ("CommonStockSharesOutstanding", "shares"),
            ("WeightedAverageNumberOfSharesOutstandingBasic", "shares"),
        ],
        "revenues": [
            ("Revenues", "USD"),
            ("RevenueFromContractWithCustomerExcludingAssessedTax", "USD"),
            ("SalesRevenueNet", "USD"),
        ],
        "debt": [
            ("LongTermDebt", "USD"),
            ("LongTermDebtNoncurrent", "USD"),
            ("DebtLongtermAndShorttermCombinedAmount", "USD"),
        ],
        "cash": [
            ("CashAndCashEquivalentsAtCarryingValue", "USD"),
            ("Cash", "USD"),
        ],
        # additional items for cashflow / capex
        "operating_cf": [("NetCashProvidedByUsedInOperatingActivities", "USD")],
        "capex_candidates": [
            ("PaymentsToAcquireProductiveAssets", "USD"),  # Amazon, others
            ("PurchasesOfPropertyPlantAndEquipment", "USD"),
            ("PaymentsToAcquirePropertyPlantAndEquipment", "USD"),
            ("AdditionsToPropertyPlantAndEquipment", "USD"),
            ("CapitalExpenditures", "USD"),
        ],
        "proceeds_candidates": [
            ("ProceedsFromSaleOfPropertyPlantAndEquipment", "USD"),
            ("ProceedsFromDispositionOfPropertyPlantAndEquipment", "USD"),
        ],
        "net_ppe": [("PropertyPlantAndEquipmentNet", "USD")],
        "gain_on_sale": [("GainOnSaleOfAsset", "USD"), ("GainLossOnDispositionOfAssets", "USD")],
    }

    out = {}

    # helper to pick first available tag in a list
    def pick_first(tag_list):
        for tag, unit in tag_list:
            v = pick_fact(cf, tag, unit, fy)
            if v is not None:
                return v, tag, unit
        return None, None, None

    # pick the basic tags
    for k in ("net_income", "depreciation", "equity", "total_liabilities", "dividends", "diluted_eps", "shares_outstanding", "revenues", "debt", "cash", "operating_cf", "net_ppe"):
        tag_list = tag_map.get(k, [])
        val, tag, unit = pick_first(tag_list)
        out[k] = val

    # capex: try direct cashflow tags first (may be negative if cash out)
    capex_val = None
    for tag, unit in tag_map["capex_candidates"]:
        capex_val = pick_fact(cf, tag, unit, fy)
        if capex_val is not None:
            out["capex_cashflow"] = capex_val
            out["capex_tag"] = tag
            break
    else:
        out["capex_cashflow"] = None

    # proceeds (sale of PPE)
    proceeds_val, ptag, punit = pick_first(tag_map["proceeds_candidates"])
    out["proceeds"] = proceeds_val or 0.0
    out["proceeds_tag"] = ptag

    # prev year net_ppe (for delta calculation)
    prev_net_ppe = None
    if out.get("net_ppe") is not None:
        prev_net_ppe = pick_fact(cf, tag_map["net_ppe"][0][0], tag_map["net_ppe"][0][1], fy - 1)
    out["prev_net_ppe"] = prev_net_ppe

    # Vorjahreswerte für Wachstum (wenn verfügbar)
    # Net Income prev
    net_income_prev = None
    ni_tag = tag_map["net_income"][0][0] if tag_map.get("net_income") else None
    ni_unit = tag_map["net_income"][0][1] if tag_map.get("net_income") else None
    if ni_tag:
        net_income_prev = pick_fact(cf, ni_tag, ni_unit, fy - 1)
    out["net_income_prev"] = net_income_prev

    # Operating CF prev
    oper_prev = None
    ocf_tag = tag_map.get("operating_cf", [(None, None)])[0][0]
    ocf_unit = tag_map.get("operating_cf", [(None, None)])[0][1]
    if ocf_tag:
        oper_prev = pick_fact(cf, ocf_tag, ocf_unit, fy - 1)
    out["operating_cf_prev"] = oper_prev

    # Prev capex: try same capex candidates for fy-1
    prev_capex_cf = None
    for tag, unit in tag_map["capex_candidates"]:
        prev_capex_cf = pick_fact(cf, tag, unit, fy - 1)
        if prev_capex_cf is not None:
            out["capex_cashflow_prev"] = prev_capex_cf
            out["capex_tag_prev"] = tag
            break
    else:
        out["capex_cashflow_prev"] = None

    # compute prev free cash flow if possible
    prev_capex_abs = None
    if out.get("capex_cashflow_prev") is not None:
        prev_capex_abs = -out["capex_cashflow_prev"] if out["capex_cashflow_prev"] < 0 else out["capex_cashflow_prev"]
    out["capex_abs_prev"] = prev_capex_abs

    if oper_prev is not None and prev_capex_abs is not None:
        out["free_cash_flow_prev"] = oper_prev - prev_capex_abs
    else:
        out["free_cash_flow_prev"] = None

    # 3-year data for CAGR: OCF and CAPEX for fy-2, fy-1, fy
    # Operating CF for fy-2
    ocf_fy_minus_2 = None
    if ocf_tag:
        ocf_fy_minus_2 = pick_fact(cf, ocf_tag, ocf_unit, fy - 2)
    out["operating_cf_fy_minus_2"] = ocf_fy_minus_2

    # CAPEX for fy-2
    capex_fy_minus_2 = None
    for tag, unit in tag_map["capex_candidates"]:
        capex_fy_minus_2 = pick_fact(cf, tag, unit, fy - 2)
        if capex_fy_minus_2 is not None:
            out["capex_fy_minus_2"] = capex_fy_minus_2
            break
    else:
        out["capex_fy_minus_2"] = None

    # CAPEX for fy-1
    capex_fy_minus_1 = None
    for tag, unit in tag_map["capex_candidates"]:
        capex_fy_minus_1 = pick_fact(cf, tag, unit, fy - 1)
        if capex_fy_minus_1 is not None:
            out["capex_fy_minus_1"] = capex_fy_minus_1
            break
    else:
        out["capex_fy_minus_1"] = None

    # CAPEX for fy (current year)
    capex_fy = None
    for tag, unit in tag_map["capex_candidates"]:
        capex_fy = pick_fact(cf, tag, unit, fy)
        if capex_fy is not None:
            out["capex_fy"] = capex_fy
            break
    else:
        out["capex_fy"] = None

    # Store 3-year OCF tuple (fy-2, fy-1, fy)
    oper = out.get("operating_cf")
    out["ocf_3year"] = (ocf_fy_minus_2, oper_prev, oper)
    # Store 3-year CAPEX tuple (fy-2, fy-1, fy)
    out["capex_3year"] = (capex_fy_minus_2, capex_fy_minus_1, capex_fy)

    # 3-year data for net_income (fy-2, fy-1, fy)
    ni_fy_minus_2 = None
    if ni_tag:
        ni_fy_minus_2 = pick_fact(cf, ni_tag, ni_unit, fy - 2)
    out["ni_3year"] = (ni_fy_minus_2, net_income_prev, out.get("net_income"))

    # 3-year data for operating_income - try to extract from SEC API
    oi_3year = (None, None, None)
    # operating_income tags (if available in XBRL)
    # Erweiterte Tag-Liste für verschiedene Unternehmen
    oi_candidates = [
        ("OperatingIncomeLoss", "USD"), 
        ("OperatingIncome", "USD"),
        ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", "USD"),
        ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign", "USD"),
        ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic", "USD"),
    ]
    oi_fy_minus_2 = None
    oi_fy_minus_1 = None
    oi_fy = None
    for tag, unit in oi_candidates:
        oi_fy_minus_2 = pick_fact(cf, tag, unit, fy - 2)
        if oi_fy_minus_2 is not None:
            break
    for tag, unit in oi_candidates:
        oi_fy_minus_1 = pick_fact(cf, tag, unit, fy - 1)
        if oi_fy_minus_1 is not None:
            break
    for tag, unit in oi_candidates:
        oi_fy = pick_fact(cf, tag, unit, fy)
        if oi_fy is not None:
            break
    out["oi_3year"] = (oi_fy_minus_2, oi_fy_minus_1, oi_fy)
    out["operating_income"] = oi_fy  # Store current year operating income

    # compute capex if not directly available
    if out.get("capex_cashflow") is None:
        net_ppe = out.get("net_ppe")
        depr = out.get("depreciation") or 0.0
        proceeds = out.get("proceeds") or 0.0
        if net_ppe is not None and prev_net_ppe is not None:
            delta_netppe = net_ppe - prev_net_ppe
            # CAPEX ≈ ΔNetPPE + Depreciation - Proceeds
            approx_capex = delta_netppe + depr - proceeds
            out["capex_approx"] = approx_capex
            out["capex_cashflow"] = approx_capex
        else:
            out["capex_approx"] = None

    # normalize capex (interpret as positive cash spent)
    capex_cf = out.get("capex_cashflow")
    if capex_cf is not None:
        # if reported as negative cash outflow, make positive magnitude
        capex_abs = -capex_cf if capex_cf < 0 else capex_cf
    else:
        capex_abs = None
    out["capex_abs"] = capex_abs

    # compute free cash flow if possible
    oper = out.get("operating_cf")
    if oper is not None and capex_abs is not None:
        out["free_cash_flow"] = oper - capex_abs
    else:
        out["free_cash_flow"] = None

    # Hole Quartalsdaten und projiziere auf Gesamtjahr
    quarterly_data = get_latest_quarterly_data(cik, fy)
    out["quarterly_data"] = quarterly_data

    return out


# ==========================
# Datenklassen
# ==========================

@dataclass
class RawFigures:
    """Rohzahlen aus dem SEC Filing"""
    company_name: str
    filing_year: int
    currency: str
    net_income: float  # in Millionen
    depreciation: float  # in Millionen
    equity: float  # in Millionen
    debt: float  # in Millionen
    cash: float
    dividends: float  # in Millionen
    diluted_eps: float  # per Share
    share_price: float
    fx_rate: float
    # optional / extended fields (defaults so existing callers still work)
    revenues: float = 0.0
    operating_income: float = 0.0
    operating_cf: float = 0.0
    capex: float = 0.0
    capex_abs: float = 0.0
    proceeds: float = 0.0
    net_ppe: float = 0.0
    prev_net_ppe: float = 0.0
    free_cash_flow: float = 0.0
    discount_rate: float = 0.07
    growth_rate: float = 0.02
    # Vorjahreswerte (falls vorhanden)
    prev_net_income: float = 0.0
    prev_operating_cf: float = 0.0
    prev_free_cash_flow: float = 0.0
    # 3-year raw data tuples (fy-2, fy-1, fy, projected) for charting
    ni_3year_tuple: tuple = (0.0, 0.0, 0.0, 0.0)  # Net Income inkl. projected
    oi_3year_tuple: tuple = (0.0, 0.0, 0.0, 0.0)  # Operating Income inkl. projected
    ocf_3year_tuple: tuple = (0.0, 0.0, 0.0, 0.0)  # Operating CF inkl. projected
    capex_3year_tuple: tuple = (0.0, 0.0, 0.0, 0.0)  # CAPEX inkl. projected
    fcf_3year_tuple: tuple = (0.0, 0.0, 0.0, 0.0)  # FCF inkl. projected
    # Quartalsprojektionen (extrapoliert auf Gesamtjahr basierend auf letztem 10-Q)
    projected_annual_ni: float = 0.0  # Projiziertes Net Income (aktuelles Jahr, basierend auf 10-Q)
    projected_annual_ocf: float = 0.0  # Projizierter Operating CF
    projected_annual_capex: float = 0.0  # Projizierter CAPEX
    projected_annual_fcf: float = 0.0  # Projizierter Free Cash Flow (OCF - CAPEX)
    has_quarterly_projection: bool = False  # Ob Quartalsprojektionen verfügbar sind
    quarterly_net_income_ytd: float = 0.0  # YTD Net Income aus letztem 10-Q
    quarterly_ocf_ytd: float = 0.0  # YTD Operating CF aus letztem 10-Q
    quarterly_capex_ytd: float = 0.0  # YTD CAPEX aus letztem 10-Q
    quarterly_fcf_ytd: float = 0.0  # YTD FCF aus letztem 10-Q (OCF - CAPEX)
    ytd_quarters: int = 0  # Anzahl enthaltener Quartale im YTD-Wert (Q1/Q2/Q3)
    quarterly_reference_10k_filing_date: str = ""  # Referenz: letztes 10-K Filing-Datum
    quarterly_period_start: str = ""  # Start der genutzten 10-Q YTD-Periode
    quarterly_period_end: str = ""  # Ende der genutzten 10-Q YTD-Periode
    quarterly_period_year: int = 0  # Kalenderjahr der genutzten YTD-Periode (aus period end)
    quarterly_filed_date: str = ""  # Filing-Datum der genutzten 10-Q Zeile
    has_quarterly_projection: bool = False  # Ob Quartalsdaten verfügbar sind


@dataclass
class ValueMetrics:
    """Berechnete Kennzahlen"""
    pe: float     # Price-to-Earnings (current year)
    trailing_pe: float  # Trailing P/E (basierend auf projizierten Quartalszahlen)
    shiller_pe: float  # Shiller P/E (3-year average)
    shiller_cpe: float  # Shiller P/CE (3-year average)
    diluted_pe: float  # Diluted P/E
    pb: float     # Price-to-Book
    dp: float     # Dividend Yield
    eq_return: float  # Eigenkapitalrendite
    umsatzrendite: float  # Umsatzrendite
    kapitalumschlag: float  # Kapitalumschlag
    equity_valuation: float = 0.0  # Diskontierte EK-Bewertung (per DCF-Shortcut)
    market_cap_euro: float = 0.0    # Market Cap in EUR (oder lokaler Währung nach FX)
    implied_growth: float = 0.0     # implizite Wachstumsrate (g), so dass EQ-Valuation = Market Cap
    # Additional ratios
    fcf_to_equity: float = 0.0
    ocf_to_equity: float = 0.0
    fcf_to_debt: float = 0.0
    capex_to_ocf: float = 0.0
    # 3-year CAGR growth rates (decimal)
    ocf_cagr_3year: float = 0.0
    capex_cagr_3year: float = 0.0
    ni_cagr_3year: float = 0.0
    oi_cagr_3year: float = 0.0



@dataclass
class FilingResult:
    """Komplettes Ergebnis: Rohzahlen + Kennzahlen"""
    raw: RawFigures
    metrics: ValueMetrics


# ==========================
# Filing-Klasse für SEC API
# ==========================

class Filing:
    """
    Repräsentiert ein 10-K Filing über SEC API.
    Zieht Daten anhand von CIK und Fiskaljahrm berechnet Kennzahlen.
    """
    
    def __init__(self, cik: int | str, filing_year: int, company_name: str, fx_rate: float = 1.0, share_price: float = None, discount_rate: float = 0.07, growth_rate: float = 0.02):
        """
        Initialisiert ein Filing mit CIK und Fiskaljahrm.
        
        Args:
            cik: SEC CIK Nummer (z.B. 1326801 für Meta)
            filing_year: Fiskaljahrm (z.B. 2024)
            company_name: Unternehmensname
            fx_rate: Umrechnungskurs USD->EUR (default: 1.0)
            share_price: Aktienkurs in USD (optional, wird sonst automatisch gezogen)
            discount_rate: Diskontierungssatz für DCF
            growth_rate: Wachstumsrate für DCF
        """
        self.cik = cik
        self.filing_year = filing_year
        self.company_name = company_name
        self.currency = "USD"  # Immer USD von SEC API
        self.fx_rate = fx_rate
        self.share_price = share_price  # kann None sein
        # store discount/growth for later valuation
        self.discount_rate = discount_rate
        self.growth_rate = growth_rate
        self.metrics_dict: Optional[dict] = None
        self.raw_figures: Optional[RawFigures] = None
        self.metrics: Optional[ValueMetrics] = None

    def _cik_to_ticker(self):
        """Sucht den Börsenticker zur CIK in company_tickers.json."""
        json_path = _resolve_company_tickers_path()
        with open(json_path, 'r') as f:
            data = json.load(f)
        for entry in data.values():
            if str(entry.get("cik_str")) == str(self.cik):
                return entry.get("ticker")
        return None

    def _get_latest_price_stooq(self, ticker):
        """Holt letzten Schlusskurs von Stooq (in USD)."""
        try:
            url = f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            if not df.empty and "Close" in df.columns:
                price = float(df["Close"].dropna().iloc[-1])
                if price > 0:
                    print(f"Aktienkurs von Stooq abgerufen: {ticker} = ${price:.2f}")
                    return price
        except Exception as e:
            print(f"Stooq Fehler für {ticker}: {e}")
        
        print(f"Konnte keinen Preis für {ticker} abrufen - bitte manuell eingeben")
        return None

    def _get_usdeur_fx_stooq(self):
        """Holt aktuellen EUR/USD FX-Kurs von Stooq."""
        try:
            url = "https://stooq.com/q/d/l/?s=eurusd&i=d"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            if not df.empty and "Close" in df.columns:
                eur_usd = float(df["Close"].dropna().iloc[-1])
                if eur_usd > 0:
                    print(f"FX-Kurs von Stooq abgerufen: EUR/USD = {eur_usd:.4f}")
                    return eur_usd
        except Exception as e:
            print(f"Stooq FX Fehler: {e}")
        
        # Ultimate fallback: reasonable default
        print("Verwende Standard-FX-Kurs: 1.15")
        return 1.15
    
    def load(self) -> dict:
        """
        Zieht Rohmetdaten vom SEC API.
        Gibt Dictionary mit net_income, depreciation, equity, dividends, diluted_eps zurück.
        """
        print(f"Lade Daten für CIK {self.cik}, Jahr {self.filing_year}...")
        self.metrics_dict = get_core_metrics_from_companyfacts(self.cik, self.filing_year)
        print(f"[OK] Daten geladen: {self.metrics_dict}")

        # Falls share_price nicht gesetzt, automatisch holen (in EUR!)
        if self.share_price is None or self.share_price == 0.0:
            ticker = self._cik_to_ticker()
            if ticker:
                price_usd = self._get_latest_price_stooq(ticker)
                fx = self._get_usdeur_fx_stooq()  # EUR/USD rate
                if price_usd is not None and fx > 0:
                    # FX is EUR/USD (how many USD for 1 EUR), so EUR = USD / FX
                    price_eur = price_usd / fx
                    self.share_price = price_eur
                    print(f"Aktienkurs automatisch gezogen: {price_usd:.2f} USD, FX (EUR/USD): {fx:.4f}, EUR: {price_eur:.2f}")
                else:
                    print("Konnte Kurs oder FX nicht automatisch bestimmen.")
                    self.share_price = 0.0  # Setze auf 0.0 statt None
            else:
                print("Kein Ticker für CIK gefunden, Kurs muss manuell übergeben werden!")
                self.share_price = 0.0  # Setze auf 0.0 statt None

        return self.metrics_dict
    
    def extract_raw_figures(self) -> RawFigures:
        """
        Erstellt RawFigures-Objekt aus den gezogenen SEC-Daten.
        
        Returns:
            RawFigures-Objekt
        """
        if self.metrics_dict is None:
            raise ValueError("Zuerst load() aufrufen!")
        
        # Werte aus SEC API (in den Einheiten, wie gemeldet)
        net_income = self.metrics_dict.get("net_income") or 0.0
        cash = self.metrics_dict.get("cash") or 0.0
        debt = self.metrics_dict.get("debt") or 0.0
        depreciation = self.metrics_dict.get("depreciation") or 0.0
        equity = self.metrics_dict.get("equity") or 0.0
        dividends = self.metrics_dict.get("dividends") or 0.0
        diluted_eps = self.metrics_dict.get("diluted_eps") or 0.0

        # optional: operating CF / capex / proceeds / net PPE
        operating_cf = self.metrics_dict.get("operating_cf") or 0.0
        capex = self.metrics_dict.get("capex_cashflow")
        capex_abs = self.metrics_dict.get("capex_abs") or 0.0
        proceeds = self.metrics_dict.get("proceeds") or 0.0
        net_ppe = self.metrics_dict.get("net_ppe") or 0.0
        prev_net_ppe = self.metrics_dict.get("prev_net_ppe") or 0.0
        free_cash_flow = self.metrics_dict.get("free_cash_flow")

        # revenues / operating_income may not be present in the pulled dict; keep defaults
        revenues = self.metrics_dict.get("revenues") or 0.0
        operating_income = self.metrics_dict.get("operating_income") or 0.0
        prev_net_income = self.metrics_dict.get("net_income_prev") or 0.0
        prev_operating_cf = self.metrics_dict.get("operating_cf_prev") or 0.0
        prev_free_cash_flow = self.metrics_dict.get("free_cash_flow_prev") or 0.0

        # 3-year tuples from metrics_dict
        ni_3year_tuple = self.metrics_dict.get("ni_3year", (None, None, None))
        ni_3year_tuple = tuple(x or 0.0 for x in ni_3year_tuple)  # Replace None with 0.0
        oi_3year_tuple = self.metrics_dict.get("oi_3year", (None, None, None))
        oi_3year_tuple = tuple(x or 0.0 for x in oi_3year_tuple)
        ocf_3year_tuple = self.metrics_dict.get("ocf_3year", (None, None, None))
        ocf_3year_tuple = tuple(x or 0.0 for x in ocf_3year_tuple)
        capex_3year_tuple = self.metrics_dict.get("capex_3year", (None, None, None))
        capex_3year_tuple = tuple(x or 0.0 for x in capex_3year_tuple)

        # Quartalsprojektionen
        quarterly_data = self.metrics_dict.get("quarterly_data", {})
        projected_annual_ni = quarterly_data.get("projected_annual_ni", 0.0)
        projected_annual_ocf = quarterly_data.get("projected_annual_ocf", 0.0)
        projected_annual_capex = quarterly_data.get("projected_annual_capex", 0.0)
        projected_annual_fcf = projected_annual_ocf - projected_annual_capex  # Trailing FCF
        has_quarterly_projection = quarterly_data.get("has_quarterly_data", False)
        quarterly_net_income_ytd = quarterly_data.get("quarterly_net_income_ytd") or 0.0
        quarterly_ocf_ytd = quarterly_data.get("quarterly_ocf_ytd") or 0.0
        quarterly_capex_ytd = quarterly_data.get("quarterly_capex_ytd") or 0.0
        quarterly_fcf_ytd = quarterly_data.get("quarterly_fcf_ytd") or 0.0
        ytd_quarters = int(quarterly_data.get("ytd_quarters") or 0)
        quarterly_reference_10k_filing_date = quarterly_data.get("filing_date_reference") or ""
        quarterly_period_start = quarterly_data.get("quarterly_period_start") or ""
        quarterly_period_end = quarterly_data.get("quarterly_period_end") or ""
        quarterly_period_year = int(quarterly_data.get("quarterly_period_year") or 0)
        quarterly_filed_date = quarterly_data.get("quarterly_filed_date") or ""
        
        # Erweitere Tupel um projizierte Werte (4. Element)
        ni_3year_tuple = ni_3year_tuple + (projected_annual_ni if has_quarterly_projection else 0.0,)
        oi_3year_tuple = oi_3year_tuple + (None,)  # Operating Income wird nicht projiziert
        ocf_3year_tuple = ocf_3year_tuple + (projected_annual_ocf if has_quarterly_projection else 0.0,)
        capex_3year_tuple = capex_3year_tuple + (projected_annual_capex if has_quarterly_projection else 0.0,)
        
        # Berechne FCF für alle 3 Jahre + Projektion
        fcf_3year_list = []
        for i in range(3):
            fcf = ocf_3year_tuple[i] - capex_3year_tuple[i]
            fcf_3year_list.append(fcf)
        fcf_3year_list.append(projected_annual_fcf if has_quarterly_projection else 0.0)
        fcf_3year_tuple = tuple(fcf_3year_list)

        self.raw_figures = RawFigures(
            company_name=self.company_name,
            filing_year=self.filing_year,
            currency=self.currency,
            net_income=net_income,
            depreciation=depreciation,
            equity=equity,
            dividends=dividends,
            diluted_eps=diluted_eps,
            share_price=self.share_price,
            fx_rate=self.fx_rate,
            revenues=revenues,
            operating_income=operating_income,
            operating_cf=operating_cf,
            capex=capex or 0.0,
            capex_abs=capex_abs,
            proceeds=proceeds,
            net_ppe=net_ppe,
            prev_net_ppe=prev_net_ppe,
            free_cash_flow=free_cash_flow or 0.0,
            prev_net_income=prev_net_income,
            prev_operating_cf=prev_operating_cf,
            prev_free_cash_flow=prev_free_cash_flow,
            ni_3year_tuple=ni_3year_tuple,
            oi_3year_tuple=oi_3year_tuple,
            ocf_3year_tuple=ocf_3year_tuple,
            capex_3year_tuple=capex_3year_tuple,
            fcf_3year_tuple=fcf_3year_tuple,
            debt=debt,
            cash=cash,
            discount_rate=self.discount_rate,
            growth_rate=self.growth_rate,
            projected_annual_ni=projected_annual_ni,
            projected_annual_ocf=projected_annual_ocf,
            projected_annual_capex=projected_annual_capex,
            projected_annual_fcf=projected_annual_fcf,
            has_quarterly_projection=has_quarterly_projection,
            quarterly_net_income_ytd=quarterly_net_income_ytd,
            quarterly_ocf_ytd=quarterly_ocf_ytd,
            quarterly_capex_ytd=quarterly_capex_ytd,
            quarterly_fcf_ytd=quarterly_fcf_ytd,
            ytd_quarters=ytd_quarters,
            quarterly_reference_10k_filing_date=quarterly_reference_10k_filing_date,
            quarterly_period_start=quarterly_period_start,
            quarterly_period_end=quarterly_period_end,
            quarterly_period_year=quarterly_period_year,
            quarterly_filed_date=quarterly_filed_date
        )

        
        print(f"[OK] RawFigures erstellt:")
        print(f"  - Net Income: ${net_income:,.0f}M")
        print(f"  - Depreciation: ${depreciation:,.0f}M")
        print(f"  - Equity: ${equity:,.0f}M")
        print(f"  - Diluted EPS: ${diluted_eps:.2f}")
        
        if has_quarterly_projection:
            print(f"\n[OK] Quartalsprojektionen (basierend auf {quarterly_data.get('ytd_quarters', 0)} Quartalen):")
            print(f"  - Projiziertes Net Income (Gesamtjahr): ${projected_annual_ni:,.0f}")
            print(f"  - Veränderung vs. 10-K: {((projected_annual_ni - net_income) / net_income * 100 if net_income != 0 else 0):.1f}%")
            print(f"  - Projizierter Operating CF: ${projected_annual_ocf:,.0f}")
            print(f"  - Veränderung vs. 10-K: {((projected_annual_ocf - operating_cf) / operating_cf * 100 if operating_cf != 0 else 0):.1f}%")
            print(f"  - Projizierter CAPEX: ${projected_annual_capex:,.0f}")
            print(f"  - Veränderung vs. 10-K: {((projected_annual_capex - capex_abs) / capex_abs * 100 if capex_abs != 0 else 0):.1f}%")
            print(f"  - Projizierter FCF (Trailing): ${projected_annual_fcf:,.0f}")
            fcf_10k = (free_cash_flow or 0.0)
            print(f"  - Veränderung vs. 10-K: {((projected_annual_fcf - fcf_10k) / fcf_10k * 100 if fcf_10k != 0 else 0):.1f}%")
        
        return self.raw_figures
    
    def process(self) -> FilingResult:
        """
        Convenience-Methode: Extrahiert RawFigures und berechnet Metrics in einem Schritt.
        Erwartet, dass load() bereits aufgerufen wurde.
        
        Returns:
            FilingResult mit RawFigures und ValueMetrics
        """
        self.extract_raw_figures()
        self.compute_metrics()
        return self.to_result()
    
    def compute_metrics(self) -> ValueMetrics:
        """
        Berechnet Kennzahlen aus RawFigures.
        
        Returns:
            ValueMetrics-Objekt
        """
        if self.raw_figures is None:
            raise ValueError("Zuerst extract_raw_figures() aufrufen!")
        
        # Helper function to calculate CAGR safely
        def safe_cagr(start_value, end_value):
            """
            Calculates CAGR: (End/Start)^(1/2) - 1
            Returns 0.0 if data is insufficient or invalid
            """
            if start_value and start_value > 0 and end_value is not None:
                try:
                    return (abs(end_value) / abs(start_value)) ** (1.0 / 2.0) - 1.0
                except (ValueError, ZeroDivisionError):
                    return 0.0
            return 0.0
        
        raw = self.raw_figures
        
        # SEC API returns values in absolute numbers (not millions)
        net_income_abs = raw.net_income
        
        # Get Shares Outstanding from SEC data directly, fallback to calculation from EPS
        shares_outstanding = self.metrics_dict.get("shares_outstanding")
        if not shares_outstanding or shares_outstanding <= 0:
            # Fallback: Approximate using Net Income / Diluted EPS
            if raw.diluted_eps and raw.diluted_eps != 0:
                shares_outstanding = abs(net_income_abs / raw.diluted_eps)
            else:
                shares_outstanding = 0

        market_cap_dollar = shares_outstanding * raw.share_price if shares_outstanding > 0 and raw.share_price > 0 else 0



        # Price-to-Earnings: Market Cap / Average Net Income (3-year)
        # Calculate 3-year average net income
        ni_3year = self.metrics_dict.get("ni_3year", (None, None, None))
        ni_values = [x for x in ni_3year if x is not None and x > 0]
        avg_net_income = sum(ni_values) / len(ni_values) if ni_values else net_income_abs
        shiller_pe = (market_cap_dollar / avg_net_income) if avg_net_income > 0 else 0
        pe = (market_cap_dollar / net_income_abs) if net_income_abs > 0 else 0
        diluted_pe = (raw.share_price / raw.diluted_eps) if raw.diluted_eps > 0 else 0
        
        # Trailing P/E basierend auf projizierten Earnings
        projected_ni = raw.projected_annual_ni
        trailing_pe = (market_cap_dollar / projected_ni) if projected_ni > 0 else pe        
        # Price-to-Cash-Earnings: Market Cap / Average Cash Earnings (3-year)
        # Calculate cash earnings for each year
        depr_abs = raw.depreciation
        cash_earnings_3year = []
        for ni in ni_3year:
            if ni is not None:
                cash_earnings_3year.append(ni + depr_abs)
        ce_values = [x for x in cash_earnings_3year if x > 0]
        avg_cash_earnings = sum(ce_values) / len(ce_values) if ce_values else (net_income_abs + depr_abs)
        shiller_cpe = (market_cap_dollar / avg_cash_earnings) if avg_cash_earnings > 0 else 0
       

        # Book-to-Market: Equity / Market Cap
        equity_abs = raw.equity
        pb = (equity_abs / market_cap_dollar) if market_cap_dollar > 0 else 0

        # Dividend Yield: Dividends / Market Cap
        dividends_abs = raw.dividends
        dp = (dividends_abs / market_cap_dollar) if market_cap_dollar > 0 else 0

        #Du-Pont
        eq_return = net_income_abs / equity_abs if equity_abs > 0 else 0
        # Umsatzrendite (Operating Margin) = Operating Income / Revenues
        revenues_abs = raw.revenues
        umsatzrendite = net_income_abs / revenues_abs if revenues_abs > 0 else 0
        # Kapitalumschlag (Asset Turnover) = Revenues / Equity
        kapitalumschlag = revenues_abs / equity_abs if equity_abs > 0 else 0

        # compute absolute free cash flow in USD



        free_cash_flow_abs = raw.free_cash_flow or 0.0
        projected_annual_fcf = raw.projected_annual_fcf
      
        
        # Equity valuation in USD and convert to EUR
        denom = (raw.discount_rate - raw.growth_rate)
        # Verwende projected FCF wenn vorhanden, sonst historischen FCF
        fcf_for_valuation = projected_annual_fcf if raw.has_quarterly_projection else free_cash_flow_abs
        equity_valuation_dollar = fcf_for_valuation / denom if denom != 0 else 0
        equity_valuation_euro = equity_valuation_dollar * raw.fx_rate if raw.fx_rate != 0 else 0

        # market cap in EUR
        market_cap_euro = market_cap_dollar * raw.fx_rate if raw.fx_rate != 0 else 0

        # implied growth g such that equity_valuation_euro == market_cap_euro
        # formula simplifies to: g = i - (free_cash_flow_abs / market_cap_dollar)
        if market_cap_dollar > 0:
            implied_growth = raw.discount_rate - (fcf_for_valuation / market_cap_dollar)
        else:
            implied_growth = 0.0

        # Additional ratios: FCF/EQ, OCF/EQ, FCF/DEBT, CAPEX/OCF
        # Verwende projected FCF wenn vorhanden, sonst historischen FCF
        fcf_for_ratios = projected_annual_fcf if raw.has_quarterly_projection else free_cash_flow_abs
        fcf_to_equity = fcf_for_ratios / equity_abs if equity_abs > 0 else 0
        
        ocf_abs = raw.operating_cf or 0.0
        ocf_to_equity = ocf_abs / equity_abs if equity_abs > 0 else 0
        
        debt_abs = raw.debt or 0.0
        fcf_to_debt = fcf_for_ratios / debt_abs if debt_abs > 0 else 0
        # CAPEX to OCF: shows what % of OCF goes to capex
        capex_to_ocf = (raw.capex_abs or 0.0) / ocf_abs if ocf_abs > 0 else 0

        # 3-year CAGR for OCF and CAPEX
        # Formula: CAGR = (Ending_Value / Beginning_Value) ^ (1/n) - 1, where n=2 (for 3 years)
        ocf_3year = self.metrics_dict.get("ocf_3year", (None, None, None))
        ocf_fy_minus_2, ocf_fy_minus_1, ocf_fy = ocf_3year
        ocf_cagr_3year = safe_cagr(ocf_fy_minus_2, ocf_fy)

        capex_3year = self.metrics_dict.get("capex_3year", (None, None, None))
        capex_fy_minus_2, capex_fy_minus_1, capex_fy = capex_3year
        capex_cagr_3year = safe_cagr(capex_fy_minus_2, capex_fy)

        # 3-year CAGR for Net Income
        ni_3year = self.metrics_dict.get("ni_3year", (None, None, None))
        ni_fy_minus_2, ni_fy_minus_1, ni_fy = ni_3year
        ni_cagr_3year = safe_cagr(ni_fy_minus_2, ni_fy)

        # 3-year CAGR for Operating Income
        oi_3year = self.metrics_dict.get("oi_3year", (None, None, None))
        oi_fy_minus_2, oi_fy_minus_1, oi_fy = oi_3year
        oi_cagr_3year = safe_cagr(oi_fy_minus_2, oi_fy)

        self.metrics = ValueMetrics(
            pe=pe,
            shiller_pe=shiller_pe,
            shiller_cpe=shiller_cpe,
            diluted_pe=diluted_pe,
            trailing_pe=trailing_pe,
            pb=pb,
            dp=dp,
            eq_return=eq_return,
            umsatzrendite=umsatzrendite,
            kapitalumschlag=kapitalumschlag,
            equity_valuation=equity_valuation_euro,
            market_cap_euro=market_cap_euro,
            implied_growth=implied_growth,
            fcf_to_equity=fcf_to_equity,
            ocf_to_equity=ocf_to_equity,
            fcf_to_debt=fcf_to_debt,
            capex_to_ocf=capex_to_ocf,
            ocf_cagr_3year=ocf_cagr_3year,
            capex_cagr_3year=capex_cagr_3year,
            ni_cagr_3year=ni_cagr_3year,
            oi_cagr_3year=oi_cagr_3year
        )

        print(f"[OK] Kennzahlen berechnet:")
        print(f"  - P/E Ratio: {pe:.6f}")
        print(f"  - C/P Ratio: {shiller_cpe:.6f}")
        print(f"  - P/B Ratio: {pb:.6f}")
        print(f"  - D/P Ratio: {dp:.6f}")
        print(f"  - EQ-Valuation (EUR): {equity_valuation_euro:,.2f}")
        print(f"  - Market-Cap (EUR): {market_cap_euro:,.2f}")
        print(f"  - Implied growth g: {implied_growth:.6f} ({implied_growth*100:.2f}%)")
        print(f"  - EK-Rendite: {eq_return:.6f}")
        print(f"  - Umsatzrendite: {umsatzrendite:.6f}")
        print(f"  - Kapitalumschlag: {kapitalumschlag:.6f}")

        return self.metrics
    
    def to_result(self) -> FilingResult:
        """
        Gibt komplettes FilingResult-Objekt zurück.
        
        Returns:
            FilingResult mit RawFigures und ValueMetrics
        """
        if self.raw_figures is None or self.metrics is None:
            raise ValueError("Zuerst extract_raw_figures() und compute_metrics() aufrufen!")
        
        return FilingResult(raw=self.raw_figures, metrics=self.metrics)


# ==========================
# Test
# ==========================

if __name__ == "__main__":
    # Test: Meta 2024
    filing = Filing(cik=1326801, filing_year=2024, company_name="Meta", share_price=400, fx_rate=1.15)
    filing.load()
    result = filing.process()

    import pickle

    with open("my_results.pkl", "wb") as f:
        pickle.dump(result, f)

    # LADEN
    #with open("my_results.pkl", "rb") as f:
    #    loaded_results = pickle.load(f)

      
    print("\n" + "="*60)
    print("ERGEBNIS")
    print("="*60)
    print(result)
    result_dict = asdict(result)
