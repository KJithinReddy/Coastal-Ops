"""
Marine conditions client — Open-Meteo Marine / Weather + NWS coastal alerts.

No API keys required (Open-Meteo free tier; NWS only needs a User-Agent).
Returns normalized dicts ready for Lakebase upsert.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("marine-client")

_OPEN_METEO_MARINE = os.environ.get(
    "OPEN_METEO_MARINE_URL", "https://marine-api.open-meteo.com/v1/marine"
)
_OPEN_METEO_WEATHER = os.environ.get(
    "OPEN_METEO_WEATHER_URL", "https://api.open-meteo.com/v1/forecast"
)
_NWS_BASE = os.environ.get("NWS_API_BASE_URL", "https://api.weather.gov")
_USER_AGENT = os.environ.get(
    "NWS_USER_AGENT",
    "(dataexpert-capstone-coastal-ops, student@example.com)",
)
_DEFAULT_TIMEOUT = 30

# Built-in coastal ports so demos work without geocoding.
DEFAULT_PORTS: list[dict[str, Any]] = [
    {
        "port_id": "miami-fl",
        "name": "Miami, FL",
        "lat": 25.7617,
        "lon": -80.1918,
        "state": "FL",
        "region": "Atlantic",
    },
    {
        "port_id": "boston-ma",
        "name": "Boston, MA",
        "lat": 42.3601,
        "lon": -71.0589,
        "state": "MA",
        "region": "Atlantic",
    },
    {
        "port_id": "seattle-wa",
        "name": "Seattle, WA",
        "lat": 47.6062,
        "lon": -122.3321,
        "state": "WA",
        "region": "Pacific",
    },
    {
        "port_id": "galveston-tx",
        "name": "Galveston, TX",
        "lat": 29.3013,
        "lon": -94.7977,
        "state": "TX",
        "region": "Gulf",
    },
    {
        "port_id": "norfolk-va",
        "name": "Norfolk, VA",
        "lat": 36.8508,
        "lon": -76.2859,
        "state": "VA",
        "region": "Atlantic",
    },
    {
        "port_id": "san-francisco-ca",
        "name": "San Francisco, CA",
        "lat": 37.7749,
        "lon": -122.4194,
        "state": "CA",
        "region": "Pacific",
    },
]

_CITY_COORDS: dict[str, tuple[float, float, str]] = {
    p["name"].lower(): (p["lat"], p["lon"], p["state"]) for p in DEFAULT_PORTS
}
_CITY_COORDS.update(
    {
        "new york, ny": (40.7128, -74.0060, "NY"),
        "houston, tx": (29.7604, -95.3698, "TX"),
        "los angeles, ca": (34.0522, -118.2437, "CA"),
        "charleston, sc": (32.7765, -79.9311, "SC"),
        "portland, me": (43.6591, -70.2568, "ME"),
    }
)

_LATLON_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$")


def classify_risk(
    wave_height_m: float | None,
    wind_speed_ms: float | None,
) -> str:
    """Simple go/no-go heuristic for coastal ops demos."""
    wave = wave_height_m if wave_height_m is not None else 0.0
    wind = wind_speed_ms if wind_speed_ms is not None else 0.0
    if wave >= 3.5 or wind >= 18.0:
        return "severe"
    if wave >= 2.5 or wind >= 14.0:
        return "high"
    if wave >= 1.5 or wind >= 10.0:
        return "moderate"
    return "low"


def _stable_id(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:28]


class MarineClient:
    """Fetch marine forecasts + coastal NWS alerts; normalize for Lakebase."""

    def __init__(self, timeout: int = _DEFAULT_TIMEOUT, user_agent: str | None = None):
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": user_agent or _USER_AGENT,
                "Accept": "application/json",
            }
        )

    def geocode(self, location: str) -> dict[str, Any]:
        """Resolve 'City, ST' or 'lat,lon' → coordinates + display name."""
        m = _LATLON_RE.match(location)
        if m:
            lat, lon = float(m.group(1)), float(m.group(2))
            return {
                "name": f"{lat:.4f},{lon:.4f}",
                "lat": lat,
                "lon": lon,
                "state": "",
            }

        key = location.strip().lower()
        if key in _CITY_COORDS:
            lat, lon, state = _CITY_COORDS[key]
            return {"name": location.strip(), "lat": lat, "lon": lon, "state": state}

        try:
            resp = self._session.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q": location,
                    "format": "json",
                    "limit": 1,
                    "countrycodes": "us",
                },
                headers={"User-Agent": _USER_AGENT},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            results = resp.json()
            time.sleep(1.0)
            if results:
                return {
                    "name": location.strip(),
                    "lat": float(results[0]["lat"]),
                    "lon": float(results[0]["lon"]),
                    "state": "",
                }
        except Exception as exc:
            logger.warning("Nominatim geocode failed for %r: %s", location, exc)

        raise ValueError(
            f"Could not geocode {location!r}. Use a built-in coastal city or 'lat,lon'."
        )

    def get_marine_forecast(self, lat: float, lon: float) -> dict[str, Any]:
        """Open-Meteo marine hourly: waves + swell."""
        resp = self._session.get(
            _OPEN_METEO_MARINE,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": ",".join(
                    [
                        "wave_height",
                        "wave_direction",
                        "wave_period",
                        "swell_wave_height",
                        "swell_wave_period",
                    ]
                ),
                "timezone": "UTC",
                "forecast_days": 3,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def get_weather_forecast(self, lat: float, lon: float) -> dict[str, Any]:
        """Open-Meteo weather hourly: wind for the same point."""
        resp = self._session.get(
            _OPEN_METEO_WEATHER,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "wind_speed_10m,wind_direction_10m,precipitation",
                "timezone": "UTC",
                "forecast_days": 3,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def get_nws_alerts(self, state: str, limit: int = 25) -> list[dict]:
        """Active NWS alerts for a US state (includes marine/coastal warnings)."""
        if not state or len(state) != 2:
            return []
        resp = self._session.get(
            f"{_NWS_BASE}/alerts/active",
            params={"area": state.upper()},
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "application/geo+json",
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        features = resp.json().get("features") or []
        return features[:limit]

    def current_conditions(self, lat: float, lon: float) -> dict[str, Any]:
        """Merge marine + wind into a single current-hour snapshot dict."""
        marine = self.get_marine_forecast(lat, lon)
        weather = self.get_weather_forecast(lat, lon)
        m_hourly = marine.get("hourly") or {}
        w_hourly = weather.get("hourly") or {}

        times = m_hourly.get("time") or []
        idx = 0
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00")
        if now in times:
            idx = times.index(now)

        def _at(series: list | None, i: int):
            if not series or i >= len(series):
                return None
            return series[i]

        wave = _at(m_hourly.get("wave_height"), idx)
        swell_h = _at(m_hourly.get("swell_wave_height"), idx)
        swell_p = _at(m_hourly.get("swell_wave_period"), idx)
        wind = _at(w_hourly.get("wind_speed_10m"), idx)
        wind_dir = _at(w_hourly.get("wind_direction_10m"), idx)
        precip = _at(w_hourly.get("precipitation"), idx)
        observed_at = _at(times, idx) or now
        risk = classify_risk(
            float(wave) if wave is not None else None,
            float(wind) if wind is not None else None,
        )

        summary = (
            f"Marine conditions at hour {observed_at} UTC: "
            f"wave height {wave if wave is not None else 'n/a'} m, "
            f"swell {swell_h if swell_h is not None else 'n/a'} m "
            f"({swell_p if swell_p is not None else 'n/a'} s), "
            f"wind {wind if wind is not None else 'n/a'} m/s "
            f"from {wind_dir if wind_dir is not None else 'n/a'}°, "
            f"precip {precip if precip is not None else 'n/a'} mm. "
            f"Operational risk level: {risk}."
        )

        return {
            "wave_height_m": float(wave) if wave is not None else None,
            "swell_wave_height_m": float(swell_h) if swell_h is not None else None,
            "swell_wave_period_s": float(swell_p) if swell_p is not None else None,
            "wind_speed_ms": float(wind) if wind is not None else None,
            "wind_direction_deg": float(wind_dir) if wind_dir is not None else None,
            "precipitation_mm": float(precip) if precip is not None else None,
            "risk_level": risk,
            "summary_text": summary,
            "observed_at": observed_at,
            "payload": {"marine": marine, "weather": weather, "hour_index": idx},
        }

    def harvest_location(
        self,
        location: str,
        lat: float | None = None,
        lon: float | None = None,
        state: str | None = None,
        limit: int = 40,
    ) -> list[dict]:
        """
        Build unstructured marine_documents for a coastal location:
        - Open-Meteo narrative forecasts (next ~24h, sampled)
        - Active NWS alerts for the state
        """
        if lat is None or lon is None:
            geo = self.geocode(location)
            lat, lon = geo["lat"], geo["lon"]
            state = state or geo.get("state")
            display = geo["name"]
        else:
            display = location

        docs: list[dict] = []
        marine = self.get_marine_forecast(lat, lon)
        weather = self.get_weather_forecast(lat, lon)
        docs.extend(self._normalize_forecast_hours(display, marine, weather, limit=24))

        if state:
            for feature in self.get_nws_alerts(state, limit=limit):
                doc = self._normalize_alert(feature, display)
                if doc:
                    docs.append(doc)

        return docs[:limit] if limit else docs

    def _normalize_forecast_hours(
        self,
        location: str,
        marine: dict,
        weather: dict,
        limit: int = 24,
    ) -> list[dict]:
        m_hourly = marine.get("hourly") or {}
        w_hourly = weather.get("hourly") or {}
        times = m_hourly.get("time") or []
        docs: list[dict] = []

        # Sample every 3 hours to keep embed volume manageable.
        for i in range(0, min(len(times), 72), 3):
            if len(docs) >= limit:
                break
            wave = (m_hourly.get("wave_height") or [None])[i] if i < len(m_hourly.get("wave_height") or []) else None
            swell = (m_hourly.get("swell_wave_height") or [None])[i] if i < len(m_hourly.get("swell_wave_height") or []) else None
            period = (m_hourly.get("swell_wave_period") or [None])[i] if i < len(m_hourly.get("swell_wave_period") or []) else None
            wind = (w_hourly.get("wind_speed_10m") or [None])[i] if i < len(w_hourly.get("wind_speed_10m") or []) else None
            wind_dir = (w_hourly.get("wind_direction_10m") or [None])[i] if i < len(w_hourly.get("wind_direction_10m") or []) else None
            t = times[i]
            risk = classify_risk(
                float(wave) if wave is not None else None,
                float(wind) if wind is not None else None,
            )
            headline = f"{location} marine forecast — {t} UTC"
            narrative = (
                f"{headline}. Wave height {wave} m, swell {swell} m with period {period} s, "
                f"wind {wind} m/s from {wind_dir} degrees. Risk assessment: {risk}. "
                f"Use this outlook when planning coastal voyages near {location}."
            )
            doc_id = "marine-" + _stable_id(location, t, "forecast")
            docs.append(
                {
                    "id": doc_id,
                    "location": location,
                    "source_type": "marine_forecast",
                    "source_url": _OPEN_METEO_MARINE,
                    "headline": headline,
                    "event": f"Marine forecast ({risk})",
                    "narrative_text": narrative,
                    "issued_at": t,
                    "effective_at": t,
                    "payload": {
                        "wave_height": wave,
                        "swell_wave_height": swell,
                        "swell_wave_period": period,
                        "wind_speed_10m": wind,
                        "wind_direction_10m": wind_dir,
                        "risk_level": risk,
                    },
                }
            )
        return docs

    def _normalize_alert(self, feature: dict, location: str) -> dict | None:
        props = feature.get("properties") or {}
        alert_id = props.get("id") or feature.get("id")
        if not alert_id:
            return None

        source_url = props.get("@id") or feature.get("id")
        if not isinstance(source_url, str) or not source_url.startswith(("http://", "https://")):
            source_url = None

        event = props.get("event") or "Coastal Alert"
        headline = props.get("headline") or event
        description = (props.get("description") or "").strip()
        instruction = (props.get("instruction") or "").strip()
        narrative_parts = [p for p in (headline, description, instruction) if p]
        narrative = "\n\n".join(narrative_parts).strip()
        if not narrative:
            return None

        return {
            "id": str(alert_id),
            "location": location,
            "source_type": "alert",
            "source_url": source_url,
            "headline": headline,
            "event": event,
            "narrative_text": narrative,
            "issued_at": props.get("sent") or props.get("effective"),
            "effective_at": props.get("effective") or props.get("onset"),
            "payload": feature,
        }
