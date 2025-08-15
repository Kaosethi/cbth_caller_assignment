"""
Database client for extracting player data from PC and Mobile game databases.
Current pipeline uses adapters/*, but this client is kept for future DB integration
and for ad-hoc QA. It supports mock CSVs (default) and has stubs for real DBs.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import logging
import pandas as pd

import config.settings as settings

logger = logging.getLogger(__name__)


# ---------- Configuration helpers ----------

DEFAULT_NO_LOGIN_WINDOW: Tuple[int, int] = (3, 7)  # hot window by default

def _get_database_config() -> Dict[str, Any]:
    """
    Pull DATABASE_CONFIG from settings if present, else return placeholders.
    Keeps this module import-safe even if DATABASE_CONFIG isn't defined yet.
    """
    return getattr(settings, "DATABASE_CONFIG", {
        "pc_db": {"host": "", "database": "", "user": "", "password": "", "port": 3306},
        "mobile_db": {"host": "", "database": "", "user": "", "password": "", "port": 3306},
    })


# ---------- Data model ----------

@dataclass
class ExtractionResult:
    success: bool
    extraction_mode: str
    no_login_window: Tuple[int, int]
    tier_scope: str
    total_raw_records: int
    total_after_filters: int
    tier_a_count: int
    non_tier_a_count: int
    tier_a_candidates: list
    non_tier_a_candidates: list
    exclusions: Dict[str, Any]
    data_source: str
    error: Optional[str] = None
    todo: Optional[str] = None


# ---------- Client ----------

class GameDatabaseClient:
    """Client for extracting player data from game databases."""

    def __init__(self):
        self.tz = settings.TIMEZONE  # informational only
        self.pc_connection = None
        self.mobile_connection = None

    # -- Connections ---------------------------------------------------------

    def connect_databases(self) -> Dict[str, Any]:
        """
        Establish connections to PC and Mobile databases if mock is off.
        Returns a summary dict; safe even without mysql-connector installed.
        """
        if settings.MOCK_DATA_CONFIG.get("use_mock_data", True):
            logger.info("Using mock data - skipping database connections.")
            return {
                "success": True,
                "pc_connected": False,
                "mobile_connected": False,
                "using_mock": True,
            }

        # Lazy import so local dev without MySQL lib still works
        try:
            import mysql.connector  # type: ignore
        except Exception as e:
            msg = f"mysql-connector not available: {e}"
            logger.error(msg)
            return {"success": False, "error": msg, "using_mock": False}

        cfg = _get_database_config()
        results = {
            "success": True,
            "pc_connected": False,
            "mobile_connected": False,
            "using_mock": False,
            "errors": [],
        }

        # Connect PC
        try:
            self.pc_connection = mysql.connector.connect(**cfg["pc_db"])  # type: ignore[arg-type]
            results["pc_connected"] = True
            logger.info("Connected to PC database.")
        except Exception as e:
            err = f"Failed to connect to PC database: {e}"
            logger.error(err)
            results["errors"].append(err)
            results["success"] = False

        # Connect Mobile
        try:
            self.mobile_connection = mysql.connector.connect(**cfg["mobile_db"])  # type: ignore[arg-type]
            results["mobile_connected"] = True
            logger.info("Connected to Mobile database.")
        except Exception as e:
            err = f"Failed to connect to Mobile database: {e}"
            logger.error(err)
            results["errors"].append(err)
            results["success"] = False

        return results

    # -- Extraction ----------------------------------------------------------

    def extract_candidates(
        self,
        no_login_window: Tuple[int, int] = DEFAULT_NO_LOGIN_WINDOW,
        mode: str = "FULL",
        tier_scope: str = "BOTH",
    ) -> ExtractionResult:
        """
        Extract candidate players from PC and Mobile sources.

        Args:
          no_login_window: (min_days, max_days) for last login window.
          mode: 'FULL' or custom flags for future logic.
          tier_scope: 'BOTH' | 'TIER_A' | 'NON_A'.

        Returns:
          ExtractionResult with counts, split lists, and exclusions summary.
        """
        logger.info(
            "Extracting candidates - Window: %s, Mode: %s, Scope: %s",
            no_login_window, mode, tier_scope
        )

        if settings.MOCK_DATA_CONFIG.get("use_mock_data", True):
            return self._extract_from_mock_data(no_login_window, mode, tier_scope)

        # Real DB path not implemented yet
        return ExtractionResult(
            success=False,
            extraction_mode=mode,
            no_login_window=no_login_window,
            tier_scope=tier_scope,
            total_raw_records=0,
            total_after_filters=0,
            tier_a_count=0,
            non_tier_a_count=0,
            tier_a_candidates=[],
            non_tier_a_candidates=[],
            exclusions={"total_excluded": 0, "exclusion_reasons": {}},
            data_source="database",
            error="Real database extraction not implemented yet",
            todo="Implement SQL queries for PC and Mobile databases",
        )

    def _extract_from_mock_data(
        self,
        no_login_window: Tuple[int, int],
        mode: str,
        tier_scope: str,
    ) -> ExtractionResult:
        """Extract candidates from mock CSV files in settings.MOCK_DATA_CONFIG."""
        try:
            pc_path = Path(settings.MOCK_DATA_CONFIG.get("pc_data_file", "data/mock/pc_data.csv"))
            mobile_path = Path(settings.MOCK_DATA_CONFIG.get("mobile_data_file", "data/mock/mobile_data.csv"))

            if not pc_path.exists() and not mobile_path.exists():
                raise FileNotFoundError(f"No mock CSVs found at {pc_path} or {mobile_path}")

            frames = []
            if pc_path.exists():
                df_pc = pd.read_csv(pc_path, dtype=str).fillna("")
                df_pc["source"] = "PC"
                frames.append(df_pc)
            if mobile_path.exists():
                df_m = pd.read_csv(mobile_path, dtype=str).fillna("")
                df_m["source"] = "Mobile"
                frames.append(df_m)

            combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

            # Ensure required columns exist
            required = ["platform", "game", "username", "phone", "calling_code"]
            for c in required:
                if c not in combined.columns:
                    combined[c] = ""

            # Derive/ensure last_login_date
            combined = self._ensure_last_login_date(combined)

            # Minimal tier flag presence for split (default NON-A)
            if "tier_flag" not in combined.columns:
                combined["tier_flag"] = ""

            # Apply filters
            filtered = self._apply_extraction_filters(combined, no_login_window, tier_scope)

            # Split into Tier A and Non‑Tier‑A
            tier_a = filtered[filtered["tier_flag"].astype(str).str.upper() == "A"].copy()
            non_a = filtered[tier_a.index.symmetric_difference(filtered.index)].copy()

            # Deduplicate by phone (keep first occurrence)
            tier_a = tier_a.drop_duplicates(subset=["phone"], keep="first")
            non_a = non_a.drop_duplicates(subset=["phone"], keep="first")

            result = ExtractionResult(
                success=True,
                extraction_mode=mode,
                no_login_window=no_login_window,
                tier_scope=tier_scope,
                total_raw_records=len(combined),
                total_after_filters=len(filtered),
                tier_a_count=len(tier_a),
                non_tier_a_count=len(non_a),
                tier_a_candidates=tier_a.to_dict("records"),
                non_tier_a_candidates=non_a.to_dict("records"),
                exclusions=self._generate_exclusion_report(combined, filtered),
                data_source="mock_csv",
            )
            logger.info(
                "Mock extraction complete - Tier A: %d, Non‑Tier‑A: %d",
                result.tier_a_count, result.non_tier_a_count
            )
            return result

        except Exception as e:
            logger.exception("Failed to extract from mock data")
            return ExtractionResult(
                success=False,
                extraction_mode=mode,
                no_login_window=no_login_window,
                tier_scope=tier_scope,
                total_raw_records=0,
                total_after_filters=0,
                tier_a_count=0,
                non_tier_a_count=0,
                tier_a_candidates=[],
                non_tier_a_candidates=[],
                exclusions={"total_excluded": 0, "exclusion_reasons": {}},
                data_source="mock_csv",
                error=str(e),
            )

    # -- Helpers -------------------------------------------------------------

    def _ensure_last_login_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure a 'last_login_date' (YYYY-MM-DD). If 'last_login_days_ago' exists, derive it.
        If only 'last_login_at' exists (datetime string), convert to date.
        """
        if "last_login_date" in df.columns:
            s = pd.to_datetime(df["last_login_date"], errors="coerce")
            df["last_login_date"] = s.dt.strftime("%Y-%m-%d").fillna("")
            return df

        if "last_login_days_ago" in df.columns:
            days = pd.to_numeric(df["last_login_days_ago"], errors="coerce").fillna(0).astype(int)
            today = pd.Timestamp.today().normalize()
            derived = today - days.map(lambda d: pd.Timedelta(days=d))
            df["last_login_date"] = pd.to_datetime(derived, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
            return df

        if "last_login_at" in df.columns:
            s = pd.to_datetime(df["last_login_at"], errors="coerce")
            df["last_login_date"] = s.dt.strftime("%Y-%m-%d").fillna("")
            return df

        # No info available; create blank (rows will be filtered out)
        df["last_login_date"] = ""
        return df

    def _apply_extraction_filters(
        self,
        data: pd.DataFrame,
        no_login_window: Tuple[int, int],
        tier_scope: str,
    ) -> pd.DataFrame:
        """
        Apply business-rule filters to raw data:
          - last_login_date within [min_days, max_days] ago (inclusive)
          - tier scope (A vs Non‑A)
          - non-empty phone
        """
        min_days, max_days = no_login_window
        today = pd.Timestamp.today().normalize()

        # Parse last_login_date
        s = pd.to_datetime(data.get("last_login_date", ""), errors="coerce")
        days_ago = (today - s).dt.days

        mask = pd.Series(True, index=data.index)
        mask &= days_ago >= min_days
        mask &= days_ago <= max_days

        # Tier scope
        tflag = data.get("tier_flag", "").astype(str).str.upper()
        if tier_scope == "TIER_A":
            mask &= tflag == "A"
        elif tier_scope == "NON_A":
            mask &= tflag != "A"

        # Non-empty phone
        phone = data.get("phone", "").astype(str).str.strip()
        mask &= phone != ""

        return data.loc[mask].copy()

    def _generate_exclusion_report(self, raw: pd.DataFrame, kept: pd.DataFrame) -> Dict[str, Any]:
        total_raw = len(raw)
        total_kept = len(kept)
        return {
            "total_excluded": max(0, total_raw - total_kept),
            "exclusion_reasons": {
                "outside_login_window": 0,  # TODO: compute by reason
                "insufficient_repeat_count": 0,
                "missing_phone": 0,
                "duplicate_phone": 0,
                "cooldown_period": 0,
            },
        }

    # -- Teardown ------------------------------------------------------------

    def close_connections(self):
        """Close database connections if open."""
        try:
            if self.pc_connection:
                self.pc_connection.close()
                logger.info("Closed PC database connection.")
        finally:
            self.pc_connection = None

        try:
            if self.mobile_connection:
                self.mobile_connection.close()
                logger.info("Closed Mobile database connection.")
        finally:
            self.mobile_connection = None


# ---------- CLI test harness -----------------------------------------------

def create_mock_player_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create sample player data for testing."""
    import random

    def sample_rows(n: int, prefix_phone: str):
        rows = []
        today = pd.Timestamp.today().normalize()
        for i in range(n):
            days = random.randint(1, 30)
            rows.append({
                "platform": "PC" if prefix_phone.startswith("08") else "Mobile",
                "game": "Cabal",
                "username": f"user_{i:03d}",
                "phone": f"{prefix_phone}{random.randint(1000000, 9999999)}",
                "calling_code": "66",
                "last_login_date": (today - pd.Timedelta(days=days)).strftime("%Y-%m-%d"),
                "tier_flag": random.choice(["", "A", "B", "C"]),
            })
        return pd.DataFrame(rows)

    pc_df = sample_rows(100, "08")
    mobile_df = sample_rows(80, "09")
    return pc_df, mobile_df


if __name__ == "__main__":
    # Create mock data for testing
    pc_df, mobile_df = create_mock_player_data()
    Path("data/mock").mkdir(parents=True, exist_ok=True)
    pc_df.to_csv("data/mock/pc_data.csv", index=False)
    mobile_df.to_csv("data/mock/mobile_data.csv", index=False)

    print(f"Created mock PC data: {len(pc_df)} records")
    print(f"Created mock Mobile data: {len(mobile_df)} records")

    client = GameDatabaseClient()
    result = client.extract_candidates()

    print(f"Extraction result: {result.success}")
    if result.success:
        print(f"Tier A candidates: {result.tier_a_count}")
        print(f"Non-Tier-A candidates: {result.non_tier_a_count}")
        print(f"Total exclusions: {result.exclusions['total_excluded']}")
