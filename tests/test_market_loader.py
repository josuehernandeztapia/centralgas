"""
Smart Market Loader Tests — Camino A

Tests placa nomenclature classification, hourly demand profiles,
seasonality factors, market intelligence functions, and the
existing_profiles builder for retention engine integration.

Note: Excel loader tests use mocks — the real 33MB NatGas file
is not loaded during CI. Integration test is marked skip.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, "/sessions/cool-focused-pasteur/mnt/Downloads/central-gas-agent")

from app.services.market_loader import (
    MarketSegment,
    SegmentParams,
    SEGMENT_PARAMS,
    HOURLY_PROFILES,
    SEASONALITY_FACTORS,
    STATION_MARKETS,
    StationMarket,
    NatGasVehicle,
    classify_placa,
    classify_natgas_segmento,
    _reclassify_by_consumo,
    market_segment_to_retention_segmento,
    build_existing_profiles,
    get_expected_hourly_volume,
    estimate_revenue_at_risk,
    get_market_summary,
    _is_ags_plaza,
)


# ============================================================
# Placa Classification Tests
# ============================================================

class TestClassifyPlaca:
    """Test placa nomenclature → MarketSegment mapping."""

    # --- COMBI AGS plates (highest priority, ground-truth validated) ---

    def test_combi_gen2_a0_pattern(self):
        """A0####A → Combi AGS Gen 2 (current, 346 known)."""
        seg, reason = classify_placa("A00123A")
        assert seg == MarketSegment.COMBI
        assert "Gen 2" in reason

    def test_combi_gen2_high_concession(self):
        seg, _ = classify_placa("A00488A")
        assert seg == MarketSegment.COMBI

    def test_combi_gen0_extinct(self):
        """A0##AAA → Combi AGS Gen 0 (extinct, 7 known)."""
        seg, reason = classify_placa("A014AAA")
        assert seg == MarketSegment.COMBI
        assert "Gen 0" in reason

    def test_combi_gen1_rare(self):
        """A###AA → Combi AGS Gen 1 (rare, 3 known)."""
        seg, reason = classify_placa("A954AA")
        assert seg == MarketSegment.COMBI
        assert "Gen 1" in reason

    def test_combi_gen1_another(self):
        seg, _ = classify_placa("A145AA")
        assert seg == MarketSegment.COMBI

    # --- TAXI patterns ---

    def test_taxi_tgh_suffix(self):
        seg, reason = classify_placa("ABC123TGH")
        assert seg == MarketSegment.TAXI
        assert "TGH" in reason

    def test_taxi_t_digits(self):
        seg, _ = classify_placa("T12345")
        assert seg == MarketSegment.TAXI

    def test_taxi_tax_pattern(self):
        seg, _ = classify_placa("TAX1234")
        assert seg == MarketSegment.TAXI

    def test_bus_prefix(self):
        seg, _ = classify_placa("BUS001")
        assert seg == MarketSegment.BUS

    def test_bus_au_prefix(self):
        seg, _ = classify_placa("AU1234")
        assert seg == MarketSegment.BUS

    def test_bus_suffix(self):
        seg, _ = classify_placa("123BUS")
        assert seg == MarketSegment.BUS

    def test_tp_ss_prefix(self):
        """SS prefix → Transporte de Personal (gubernamental)."""
        seg, _ = classify_placa("SS1234")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_tp_gs_prefix(self):
        """GS prefix → Transporte de Personal (gubernamental)."""
        seg, _ = classify_placa("GS5678")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_combi_t_suffix(self):
        """Plate ending in T → Combi (Transporte)."""
        seg, _ = classify_placa("AB1234T")
        assert seg == MarketSegment.COMBI

    def test_combi_a_a_pattern(self):
        """A_____A pattern → Combi AGS."""
        seg, _ = classify_placa("A1234A")
        assert seg == MarketSegment.COMBI

    def test_combi_a_longer_a(self):
        seg, _ = classify_placa("ABCDEA")
        assert seg == MarketSegment.COMBI

    def test_combi_ags_prefix(self):
        seg, _ = classify_placa("AGS123")
        assert seg == MarketSegment.COMBI

    def test_particular_standard_plate(self):
        """Standard Mexican plate: 3 letters + 3-4 digits."""
        seg, _ = classify_placa("BCD1234")
        assert seg == MarketSegment.PARTICULAR

    def test_particular_numeric(self):
        """Numeric format: ###AAA."""
        seg, _ = classify_placa("123ABC")
        assert seg == MarketSegment.PARTICULAR

    def test_unknown_empty(self):
        seg, reason = classify_placa("")
        assert seg == MarketSegment.UNKNOWN
        assert "empty" in reason

    def test_unknown_weird_plate(self):
        seg, _ = classify_placa("???")
        assert seg == MarketSegment.UNKNOWN

    def test_dash_stripped(self):
        """Dashes and spaces should be stripped."""
        seg, _ = classify_placa("A-123-4A")
        assert seg == MarketSegment.COMBI

    def test_lowercase_normalized(self):
        """Input should be uppercased."""
        seg, _ = classify_placa("t12345")
        assert seg == MarketSegment.TAXI

    def test_combi_single_letter_prefix_t_suffix(self):
        """Single letter prefix + digits + T → Combi."""
        seg, _ = classify_placa("C999T")
        assert seg == MarketSegment.COMBI

    # --- New patterns from NatGas data analysis ---

    def test_taxi_ags_aaa_suffix_pattern(self):
        """A###AA[A-K] → Taxi AGS (75% of 1,149 taxis in NatGas).
        Reclassified from COMBI after ground-truth validation:
        738 taxis in BASE_CLIENTES have this pattern."""
        seg, reason = classify_placa("A623AAH")
        assert seg == MarketSegment.TAXI
        assert "Taxi" in reason

    def test_taxi_ags_aab(self):
        seg, _ = classify_placa("A946AAB")
        assert seg == MarketSegment.TAXI

    def test_taxi_ags_aad(self):
        seg, _ = classify_placa("A179AAD")
        assert seg == MarketSegment.TAXI

    def test_combi_natgas_egx_suffix(self):
        """A###EGT/EGS → Combi NatGas."""
        seg, _ = classify_placa("A681EGT")
        assert seg == MarketSegment.COMBI

    def test_combi_federal_a_prefix(self):
        """AXX###X → Combi federal plate (A-prefix, 254 of 329)."""
        seg, _ = classify_placa("AEA898B")
        assert seg == MarketSegment.COMBI

    def test_particular_federal_plate(self):
        """XXX###X → Federal plate (non-A prefix)."""
        seg, _ = classify_placa("UZE651C")
        assert seg == MarketSegment.PARTICULAR

    def test_conversion_fleet_plate(self):
        """AX####X → Fleet/Conversion transport plate."""
        seg, _ = classify_placa("AD6959B")
        assert seg == MarketSegment.CONVERSION

    def test_tp_tg_prefix(self):
        """TG+digits → Transporte de Personal fleet."""
        seg, _ = classify_placa("TG2143")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_combi_pure_numeric(self):
        """Pure 5-7 digit NatGas ID → Combi (90% Público)."""
        seg, _ = classify_placa("155746")
        assert seg == MarketSegment.COMBI

    def test_particular_mixed_digits_letters(self):
        """##XX## → Particular."""
        seg, _ = classify_placa("096RR4")
        assert seg == MarketSegment.PARTICULAR

    # --- Round 2 patterns (from 266 remaining UNKNOWN analysis) ---

    def test_combi_natgas_id_no_prefix(self):
        """####AAB/GMJ/GMK → Combi NatGas ID without A prefix (105 vehicles)."""
        seg, _ = classify_placa("2239AAB")
        assert seg == MarketSegment.COMBI

    def test_combi_natgas_gmj(self):
        seg, _ = classify_placa("4767GMJ")
        assert seg == MarketSegment.COMBI

    def test_combi_natgas_gmk(self):
        seg, _ = classify_placa("8539GMK")
        assert seg == MarketSegment.COMBI

    def test_tp_fleet_code_cm(self):
        """####CM → Transporte de Personal fleet code (96% Empresa)."""
        seg, _ = classify_placa("9887CM")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_tp_fleet_code_cb(self):
        seg, _ = classify_placa("4521CB")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_combi_rb_serial(self):
        """##RB#Z### → Combi NatGas serial (85% Público)."""
        seg, _ = classify_placa("29RB5Z217")
        assert seg == MarketSegment.COMBI

    def test_particular_state_plate(self):
        """##XX#X → Particular state plate."""
        seg, _ = classify_placa("51AK1W")
        assert seg == MarketSegment.PARTICULAR

    def test_combi_af_series(self):
        """AFx#### → Combi AGS federal series."""
        seg, _ = classify_placa("AFN8507")
        assert seg == MarketSegment.COMBI

    def test_combi_ab_variant(self):
        """A/B###XXX → Combi variante."""
        seg, _ = classify_placa("A161TGE")
        assert seg == MarketSegment.COMBI

    def test_combi_b_variant(self):
        seg, _ = classify_placa("B199DAG")
        assert seg == MarketSegment.COMBI

    def test_combi_rb_short(self):
        """##Rx#x → Combi NatGas serial (short)."""
        seg, _ = classify_placa("04RB7Z")
        assert seg == MarketSegment.COMBI

    def test_tp_tc_prefix(self):
        """TC+digits → Transporte de Personal fleet."""
        seg, _ = classify_placa("TC102")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL


# ============================================================
# Segment Mapping Tests
# ============================================================

class TestSegmentMapping:
    """Test MarketSegment → retention Segmento mapping."""

    def test_combi_to_vagoneta(self):
        assert market_segment_to_retention_segmento(MarketSegment.COMBI) == "VAGONETA"

    def test_tp_to_vagoneta(self):
        """Transporte de Personal → VAGONETA for retention."""
        assert market_segment_to_retention_segmento(MarketSegment.TRANSPORTE_PERSONAL) == "VAGONETA"

    def test_conversion_to_vagoneta(self):
        assert market_segment_to_retention_segmento(MarketSegment.CONVERSION) == "VAGONETA"

    def test_bus_to_vagoneta(self):
        assert market_segment_to_retention_segmento(MarketSegment.BUS) == "VAGONETA"

    def test_taxi_to_taxi(self):
        assert market_segment_to_retention_segmento(MarketSegment.TAXI) == "TAXI"

    def test_particular_to_particular(self):
        assert market_segment_to_retention_segmento(MarketSegment.PARTICULAR) == "PARTICULAR"

    def test_plataforma_to_taxi(self):
        """PLATAFORMA maps to TAXI for retention thresholds."""
        assert market_segment_to_retention_segmento(MarketSegment.PLATAFORMA) == "TAXI"

    def test_unknown_to_particular(self):
        assert market_segment_to_retention_segmento(MarketSegment.UNKNOWN) == "PARTICULAR"


# ============================================================
# Hourly Profile Tests
# ============================================================

class TestHourlyProfiles:
    """Verify hourly demand profiles from Modelo Operativo v11."""

    def test_combi_profile_exists(self):
        assert "COMBI" in HOURLY_PROFILES
        assert len(HOURLY_PROFILES["COMBI"]) == 24

    def test_taxi_profile_exists(self):
        assert "TAXI" in HOURLY_PROFILES
        assert len(HOURLY_PROFILES["TAXI"]) == 24

    def test_tp_profile_exists(self):
        assert "TP" in HOURLY_PROFILES
        assert len(HOURLY_PROFILES["TP"]) == 24

    def test_bus_profile_exists(self):
        assert "BUS" in HOURLY_PROFILES
        assert len(HOURLY_PROFILES["BUS"]) == 24

    def test_combi_sums_to_approximately_one(self):
        total = sum(HOURLY_PROFILES["COMBI"].values())
        assert 0.95 <= total <= 1.05, f"COMBI profile sums to {total}"

    def test_taxi_sums_to_approximately_one(self):
        total = sum(HOURLY_PROFILES["TAXI"].values())
        assert 0.95 <= total <= 1.05, f"TAXI profile sums to {total}"

    def test_tp_sums_to_approximately_one(self):
        total = sum(HOURLY_PROFILES["TP"].values())
        assert 0.95 <= total <= 1.05, f"TP profile sums to {total}"

    def test_bus_sums_to_approximately_one(self):
        total = sum(HOURLY_PROFILES["BUS"].values())
        assert 0.95 <= total <= 1.05, f"BUS profile sums to {total}"

    def test_combi_am_peak(self):
        """Combi peak AM: hours 8-9 should be highest morning window."""
        profile = HOURLY_PROFILES["COMBI"]
        am_peak = profile[8] + profile[9]
        assert am_peak > 0.15, f"Combi AM peak too low: {am_peak}"

    def test_taxi_nocturnal(self):
        """Taxi has significant nocturnal activity (16.5%)."""
        profile = HOURLY_PROFILES["TAXI"]
        nocturnal = sum(profile[h] for h in [0, 1, 2, 3, 4, 23])
        assert nocturnal > 0.10, f"Taxi nocturnal too low: {nocturnal}"

    def test_bus_only_two_windows(self):
        """Bus charges only in AM 6-8 and PM 21-23 windows."""
        profile = HOURLY_PROFILES["BUS"]
        # All non-window hours should be 0
        for h in [0, 1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]:
            assert profile[h] == 0.0, f"Bus should be 0 at hour {h}"


# ============================================================
# Seasonality Tests
# ============================================================

class TestSeasonality:
    """Verify seasonality factors from CAPA2 2025."""

    def test_all_months_present(self):
        assert len(SEASONALITY_FACTORS) == 12
        for m in range(1, 13):
            assert m in SEASONALITY_FACTORS

    def test_july_is_peak(self):
        """July is the absolute peak (×1.150)."""
        assert SEASONALITY_FACTORS[7] == 1.150
        assert SEASONALITY_FACTORS[7] == max(SEASONALITY_FACTORS.values())

    def test_december_is_valley(self):
        """December is the valley (×0.841)."""
        assert SEASONALITY_FACTORS[12] == 0.841
        assert SEASONALITY_FACTORS[12] == min(SEASONALITY_FACTORS.values())

    def test_factors_average_near_one(self):
        """Seasonality factors should average close to 1.0."""
        avg = sum(SEASONALITY_FACTORS.values()) / 12
        assert 0.95 <= avg <= 1.05, f"Average seasonality {avg}"


# ============================================================
# Station Market Tests
# ============================================================

class TestStationMarkets:
    """Verify station market configurations."""

    def test_three_stations(self):
        assert len(STATION_MARKETS) == 3
        assert set(STATION_MARKETS.keys()) == {1, 2, 3}

    def test_pension_market(self):
        """Pensión: 500K LEQ/month, 32% combi / 68% taxi."""
        sm = STATION_MARKETS[3]
        assert sm.name == "Pensión/Nacozari"
        assert sm.total_leq_month == 500_000
        assert MarketSegment.COMBI in sm.segment_mix
        assert MarketSegment.TAXI in sm.segment_mix
        assert sm.competitor_name == "NatGas Nacozari"
        assert sm.competitor_distance_m == 100

    def test_oriente_market(self):
        """Oriente: taxis + buses, competitor Ojo Caliente at 300m."""
        sm = STATION_MARKETS[2]
        assert sm.name == "Oriente"
        assert MarketSegment.TAXI in sm.segment_mix
        assert MarketSegment.BUS in sm.segment_mix
        assert sm.competitor_distance_m == 300

    def test_parques_market(self):
        """Parques: Transporte de Personal (CETE + others) + conversions."""
        sm = STATION_MARKETS[1]
        assert sm.name == "Parques Industriales"
        assert MarketSegment.TRANSPORTE_PERSONAL in sm.segment_mix
        assert sm.competitor_distance_m == 0


# ============================================================
# Segment Params Tests
# ============================================================

class TestSegmentParams:
    """Verify segment parameters from Modelo Operativo v11."""

    def test_combi_params(self):
        """Validated from 388,665 txns: mediana=28.1 LEQ/carga, ~900 LEQ/mes."""
        p = SEGMENT_PARAMS[MarketSegment.COMBI]
        assert p.leq_per_charge == 28
        assert p.leq_per_month == 900
        assert p.cycle_minutes == 5

    def test_taxi_params(self):
        """Validated from 4,786 AGS vehicles: mediana=11.0 LEQ/carga."""
        p = SEGMENT_PARAMS[MarketSegment.TAXI]
        assert p.leq_per_charge == 11
        assert p.leq_per_month == 300
        assert p.cycle_minutes == 3

    def test_bus_params(self):
        p = SEGMENT_PARAMS[MarketSegment.BUS]
        assert p.leq_per_charge == 200
        assert p.leq_per_month == 6000
        assert p.cycle_minutes == 45

    def test_combi_revenue(self):
        """Combi lost = 900 LEQ × $13.99 = $12,591/month."""
        p = SEGMENT_PARAMS[MarketSegment.COMBI]
        assert p.revenue_per_month_mxn == pytest.approx(12591.0, rel=0.01)

    def test_bus_revenue(self):
        """Bus lost = $83,940/month."""
        p = SEGMENT_PARAMS[MarketSegment.BUS]
        assert p.revenue_per_month_mxn == pytest.approx(83940.0, rel=0.01)


# ============================================================
# Market Intelligence Functions
# ============================================================

class TestMarketIntelligence:
    """Test intelligence computation functions."""

    def test_hourly_volume_combi_peak(self):
        """Peak hour volume should be significantly higher."""
        peak_vol = get_expected_hourly_volume(
            MarketSegment.COMBI, 100_000, hour=8, month=1
        )
        off_peak_vol = get_expected_hourly_volume(
            MarketSegment.COMBI, 100_000, hour=2, month=1
        )
        assert peak_vol > off_peak_vol * 10

    def test_hourly_volume_with_seasonality(self):
        """July volume should be ~15% higher than August."""
        july = get_expected_hourly_volume(
            MarketSegment.COMBI, 100_000, hour=8, month=7
        )
        aug = get_expected_hourly_volume(
            MarketSegment.COMBI, 100_000, hour=8, month=8
        )
        assert july / aug == pytest.approx(1.15, rel=0.01)

    def test_estimate_revenue_combi(self):
        """Default combi revenue: 900 LEQ × $13.99 = $12,591."""
        rev = estimate_revenue_at_risk(MarketSegment.COMBI)
        assert rev == pytest.approx(12591.0, rel=0.01)

    def test_estimate_revenue_with_actual(self):
        """With actual litros, use that instead of default."""
        rev = estimate_revenue_at_risk(MarketSegment.COMBI, monthly_litros=500)
        assert rev == pytest.approx(6995.0, rel=0.01)

    def test_estimate_revenue_unknown_segment(self):
        rev = estimate_revenue_at_risk(MarketSegment.UNKNOWN)
        assert rev == 0.0

    def test_market_summary_structure(self):
        summary = get_market_summary()
        assert "stations" in summary
        assert "segments" in summary
        assert "seasonality" in summary
        assert len(summary["stations"]) == 3
        assert len(summary["seasonality"]) == 12


# ============================================================
# AGS Plaza Detection Tests
# ============================================================

class TestAgsPlaza:
    """Test AGS plaza keyword matching."""

    def test_aguascalientes(self):
        assert _is_ags_plaza("Aguascalientes") is True

    def test_nacozari(self):
        assert _is_ags_plaza("NACOZARI") is True

    def test_pension(self):
        assert _is_ags_plaza("Pensión") is True

    def test_ojo_caliente(self):
        assert _is_ags_plaza("Ojo Caliente") is True

    def test_queretaro(self):
        assert _is_ags_plaza("Querétaro") is False

    def test_empty(self):
        assert _is_ags_plaza("") is False


# ============================================================
# NatGas Segment Hint Tests
# ============================================================

class TestClassifyNatGasSegmento:
    """Test NatGas Desc_Segmento → MarketSegment mapping (primary classifier)."""

    def test_taxi(self):
        seg, _ = classify_natgas_segmento("Taxi")
        assert seg == MarketSegment.TAXI

    def test_taxi_inteligente(self):
        """Taxi inteligente = PLATAFORMA (DiDi/Uber, federal plates)."""
        seg, _ = classify_natgas_segmento("Taxi inteligente")
        assert seg == MarketSegment.PLATAFORMA

    def test_taxi_ejecutivo(self):
        """Taxi ejecutivo = PLATAFORMA (DiDi/Uber, federal plates)."""
        seg, _ = classify_natgas_segmento("Taxi ejecutivo")
        assert seg == MarketSegment.PLATAFORMA

    def test_combis_colectivas(self):
        seg, _ = classify_natgas_segmento("Combis Colectivas")
        assert seg == MarketSegment.COMBI

    def test_camion_colectivo(self):
        """Camión Colectivo = BUS (consumo prom 161 lt/day, NOT combi!)."""
        seg, _ = classify_natgas_segmento("Camión Colectivo")
        assert seg == MarketSegment.BUS

    def test_transporte_personal(self):
        seg, _ = classify_natgas_segmento("Transporte de personal")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_empresa(self):
        """Empresa = corporate fleet → Transporte de Personal."""
        seg, _ = classify_natgas_segmento("Empresa")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_publico_defaults_taxi(self):
        """Público in AGS is predominantly taxi."""
        seg, _ = classify_natgas_segmento("Público")
        assert seg == MarketSegment.TAXI

    def test_privado(self):
        seg, _ = classify_natgas_segmento("Privado")
        assert seg == MarketSegment.PARTICULAR

    def test_particular(self):
        seg, _ = classify_natgas_segmento("Particular")
        assert seg == MarketSegment.PARTICULAR

    def test_plataforma(self):
        """Explicit Plataforma label from NatGas."""
        seg, _ = classify_natgas_segmento("Plataforma")
        assert seg == MarketSegment.PLATAFORMA

    def test_gobierno(self):
        seg, _ = classify_natgas_segmento("Gobierno")
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_transporte_urbano(self):
        seg, _ = classify_natgas_segmento("Transporte Urbano")
        assert seg == MarketSegment.BUS

    def test_ninguno(self):
        seg, _ = classify_natgas_segmento("Ninguno")
        assert seg == MarketSegment.UNKNOWN

    def test_empty(self):
        seg, _ = classify_natgas_segmento("")
        assert seg == MarketSegment.UNKNOWN

    def test_mypime(self):
        seg, _ = classify_natgas_segmento("Mypime")
        assert seg == MarketSegment.PARTICULAR

    def test_carga(self):
        seg, _ = classify_natgas_segmento("Transporte de carga")
        assert seg == MarketSegment.BUS


# ============================================================
# Consumo-Based Reclassification Tests
# ============================================================

class TestReclassifyByConsumo:
    """Test consumo-based reclassification of ambiguous NatGas segments.

    Thresholds validated against real BASE_CLIENTES_GNC_AGUASCALIENTES.xlsx:
      - VAGONETAS file: 331 combis/vagonetas, max Lt/carga 15.5-70 LEQ
      - Only 56 "Camión Colectivo" with consumo_max > 200 are real buses
      - 14 "Camión Colectivo" in VAGONETAS file have consumo_max ≤ 51
    """

    # ── Camión Colectivo correction ──

    def test_camion_colectivo_high_consumo_stays_bus(self):
        """Camión Colectivo with consumo_max > 200 = real bus."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.BUS, "NatGas: Camión Colectivo → Bus (consumo >50)",
            Decimal("500"), Decimal("161"), "Camión Colectivo", "BUS001",
        )
        assert seg == MarketSegment.BUS

    def test_camion_colectivo_low_consumo_becomes_tp(self):
        """Camión Colectivo with consumo_max ≤ 200 and fleet plate → TP.
        Note: A0####A plates are caught by the plate override BEFORE this
        function, so they arrive already reclassified to COMBI."""
        seg, reason = _reclassify_by_consumo(
            MarketSegment.BUS, "NatGas: Camión Colectivo → Bus (consumo >50)",
            Decimal("48"), Decimal("30"), "Camión Colectivo", "YV1134",
        )
        assert seg == MarketSegment.TRANSPORTE_PERSONAL
        assert "fleet" in reason

    def test_camion_colectivo_mid_consumo_becomes_tp(self):
        """Camión Colectivo with consumo_max 100-200 and fleet plate → TP."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.BUS, "NatGas: Camión Colectivo → Bus (consumo >50)",
            Decimal("150"), Decimal("80"), "Camión Colectivo", "K1003838",
        )
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_camion_colectivo_combi_plate_stays_combi(self):
        """Camión Colectivo with A0####A plate → COMBI (plate override)."""
        # The A0####A override runs BEFORE _reclassify_by_consumo in the
        # loader pipeline, so it arrives as COMBI already. Test that the
        # override works correctly via the plate check in step 0.
        seg, reason = _reclassify_by_consumo(
            MarketSegment.BUS, "NatGas: Camión Colectivo → Bus (consumo >50)",
            Decimal("48"), Decimal("30"), "Camión Colectivo", "A00137A",
        )
        # A0####A triggers the plate override → COMBI
        assert seg == MarketSegment.COMBI
        assert "AGS combi plate" in reason

    def test_camion_colectivo_no_consumo_stays_bus(self):
        """Camión Colectivo with no consumo data → conservative: stays BUS."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.BUS, "NatGas: Camión Colectivo → Bus (consumo >50)",
            Decimal("0"), Decimal("0"), "Camión Colectivo", "X001",
        )
        assert seg == MarketSegment.BUS

    # ── Empresa: fleet vehicles, NOT buses ──

    def test_empresa_high_consumo_stays_tp(self):
        """Empresa stays as TP regardless of high consumo (fleet, not bus)."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.TRANSPORTE_PERSONAL, "NatGas: Empresa → TP/fleet",
            Decimal("300"), Decimal("150"), "Empresa", "XY1234",
        )
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_empresa_moderate_consumo_stays_tp(self):
        """Empresa with moderate consumo stays TP."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.TRANSPORTE_PERSONAL, "NatGas: Empresa → TP/fleet",
            Decimal("40"), Decimal("30"), "Empresa", "TG1234",
        )
        assert seg == MarketSegment.TRANSPORTE_PERSONAL

    def test_empresa_very_low_consumo_becomes_particular(self):
        """Empresa with consumo_max ≤ 16 → PARTICULAR (personal vehicle)."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.TRANSPORTE_PERSONAL, "NatGas: Empresa → TP/fleet",
            Decimal("12"), Decimal("8"), "Empresa", "BCD1234",
        )
        assert seg == MarketSegment.PARTICULAR

    # ── Público: PLATAFORMA detection by federal plate ──

    def test_publico_federal_plate_becomes_plataforma(self):
        """Público with federal plate (XXX###X) → PLATAFORMA."""
        seg, reason = _reclassify_by_consumo(
            MarketSegment.TAXI, "NatGas: Público → Taxi (público)",
            Decimal("17"), Decimal("12"), "Público", "AAY054C",
        )
        assert seg == MarketSegment.PLATAFORMA
        assert "federal plate" in reason

    def test_publico_normal_plate_stays_taxi(self):
        """Público with regular plate stays as TAXI."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.TAXI, "NatGas: Público → Taxi (público)",
            Decimal("16"), Decimal("11"), "Público", "A623AAH",
        )
        assert seg == MarketSegment.TAXI

    def test_publico_high_consumo_stays_taxi(self):
        """Público with high consumo stays TAXI (NOT reclassified to BUS)."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.TAXI, "NatGas: Público → Taxi (público)",
            Decimal("120"), Decimal("80"), "Público", "XYZ123",
        )
        assert seg == MarketSegment.TAXI

    # ── Other segments: no aggressive reclassification ──

    def test_taxi_stays_taxi_regardless_of_consumo(self):
        """TAXI stays TAXI — no general consumo→BUS reclassification."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.TAXI, "NatGas: Taxi",
            Decimal("60"), Decimal("40"), "Taxi", "T1234",
        )
        assert seg == MarketSegment.TAXI

    def test_combi_stays_combi_even_high_consumo(self):
        """COMBI stays COMBI — vagonetas legitimately reach 70 LEQ/charge."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.COMBI, "NatGas: Combis Colectivas",
            Decimal("70"), Decimal("40"), "Combis Colectivas", "A02350A",
        )
        assert seg == MarketSegment.COMBI

    def test_zero_consumo_no_change(self):
        """Zero consumo doesn't trigger reclassification."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.TAXI, "NatGas: Taxi",
            Decimal("0"), Decimal("0"), "Taxi", "T1234",
        )
        assert seg == MarketSegment.TAXI

    def test_plataforma_stays_plataforma(self):
        """PLATAFORMA stays PLATAFORMA — no consumo override."""
        seg, _ = _reclassify_by_consumo(
            MarketSegment.PLATAFORMA, "NatGas: Taxi inteligente",
            Decimal("60"), Decimal("40"), "Taxi inteligente", "JRG5329",
        )
        assert seg == MarketSegment.PLATAFORMA


# ============================================================
# Build Existing Profiles Tests
# ============================================================

class TestBuildExistingProfiles:
    """Test the existing_profiles builder for retention engine."""

    def _make_vehicles(self) -> list[NatGasVehicle]:
        return [
            NatGasVehicle(
                placa="A1234A",
                marca="Nissan",
                linea="Urvan",
                natgas_segmento="Combi",
                market_segment=MarketSegment.COMBI,
                classification_reason="A____A → Combi AGS",
                consumo_promedio=Decimal("35"),
                plaza="Nacozari",
                is_ags=True,
            ),
            NatGasVehicle(
                placa="T5678",
                natgas_segmento="Taxi",
                market_segment=MarketSegment.TAXI,
                classification_reason="T+digits → Taxi",
                consumo_promedio=Decimal("10"),
                plaza="Ojo Caliente",
                is_ags=True,
            ),
            NatGasVehicle(
                placa="QRO001",
                market_segment=MarketSegment.COMBI,
                plaza="Querétaro",
                is_ags=False,  # Not AGS
            ),
        ]

    def test_ags_only_filter(self):
        vehicles = self._make_vehicles()
        profiles = build_existing_profiles(vehicles, ags_only=True)
        assert "A1234A" in profiles
        assert "T5678" in profiles
        assert "QRO001" not in profiles

    def test_all_plazas(self):
        vehicles = self._make_vehicles()
        profiles = build_existing_profiles(vehicles, ags_only=False)
        assert len(profiles) == 3

    def test_segmento_mapping(self):
        vehicles = self._make_vehicles()
        profiles = build_existing_profiles(vehicles, ags_only=True)
        assert profiles["A1234A"]["segmento"] == "VAGONETA"
        assert profiles["T5678"]["segmento"] == "TAXI"

    def test_market_segment_preserved(self):
        vehicles = self._make_vehicles()
        profiles = build_existing_profiles(vehicles, ags_only=True)
        assert profiles["A1234A"]["market_segment"] == "COMBI"

    def test_consumo_esperado(self):
        """Monthly estimate: daily avg × 30."""
        vehicles = self._make_vehicles()
        profiles = build_existing_profiles(vehicles, ags_only=True)
        assert profiles["A1234A"]["consumo_esperado_lt"] == 35 * 30

    def test_consumo_data_enrichment(self):
        """External consumption data overrides vehicle default."""
        vehicles = self._make_vehicles()
        consumo = {"A1234A": {"consumo_promedio": Decimal("50"), "consumo_max": Decimal("70")}}
        profiles = build_existing_profiles(vehicles, consumo_data=consumo, ags_only=True)
        assert profiles["A1234A"]["consumo_esperado_lt"] == 50 * 30

    def test_inactive_flag(self):
        vehicles = self._make_vehicles()
        inactive = {"T5678": {"is_inactive": True, "plaza": "Ojo Cal", "segmento": "Taxi"}}
        profiles = build_existing_profiles(vehicles, inactive_data=inactive, ags_only=True)
        assert profiles["T5678"]["is_natgas_inactive"] is True
        assert profiles["T5678"]["estatus"] == "INACTIVO_NATGAS"

    def test_default_consumo_from_segment(self):
        """If no consumption data, use segment default."""
        vehicles = [
            NatGasVehicle(
                placa="NEW001",
                market_segment=MarketSegment.COMBI,
                plaza="AGS Central",
                is_ags=True,
                consumo_promedio=Decimal("0"),
            ),
        ]
        profiles = build_existing_profiles(vehicles, ags_only=True)
        # Should use COMBI default of 900 LEQ/month (validated from real data)
        assert profiles["NEW001"]["consumo_esperado_lt"] == 900


# ============================================================
# Orchestrator Retention Integration Test
# ============================================================

class TestOrchestratorRetention:
    """Test the RETENTION phase in the cron orchestrator pipeline."""

    def _make_config(self):
        from app.services.orchestrator import OrchestratorConfig
        return OrchestratorConfig(
            csv_dir=Path("/tmp/test_csv"),
            odoo_enabled=False,
            scada_enabled=False,
            whatsapp_enabled=False,
            db_enabled=False,
        )

    def _make_txn(self, placa, station_id=3, local_date=date(2026, 1, 15),
                  litros=40.0, total=559.60):
        from app.models.transaction import (
            TransactionNormalized, MedioPago, SchemaVersion,
        )
        CST = timezone(timedelta(hours=-6))
        ts_local = datetime(local_date.year, local_date.month, local_date.day,
                            10, 0, 0, tzinfo=CST)
        ts_utc = ts_local.astimezone(timezone.utc)
        litros_d = Decimal(str(litros))
        total_d = Decimal(str(total))
        pvp = (total_d / litros_d).quantize(Decimal("0.01")) if litros_d else Decimal("0")
        kg = (litros_d * Decimal("0.717")).quantize(Decimal("0.001"))
        nm3 = litros_d
        neto = (total_d / Decimal("1.16")).quantize(Decimal("0.01"))
        iva = total_d - neto
        return TransactionNormalized(
            source_file="test.csv", source_row=1,
            schema_version=SchemaVersion.POST_2023,
            station_id=station_id, station_natgas="TEST",
            timestamp_utc=ts_utc, timestamp_local=ts_local,
            placa=placa, litros=litros_d, pvp=pvp, total_mxn=total_d,
            medio_pago=MedioPago.EFECTIVO, kg=kg, nm3=nm3,
            ingreso_neto=neto, iva=iva,
        )

    def test_retention_phase_runs(self):
        """RETENTION phase should run and produce results."""
        from app.services.orchestrator import Orchestrator, Phase, PhaseStatus

        config = self._make_config()

        # Market profiles from Smart Loader
        market_profiles = {
            "COMBI-001": {"segmento": "VAGONETA", "market_segment": "COMBI"},
            "TAXI-001": {"segmento": "TAXI", "market_segment": "TAXI"},
        }

        orch = Orchestrator(config, market_profiles=market_profiles)

        # Simulate parsed transactions
        txns = [
            self._make_txn("COMBI-001", station_id=3, local_date=date(2026, 1, 29)),
            self._make_txn("COMBI-001", station_id=3, local_date=date(2026, 1, 15)),
            self._make_txn("TAXI-001", station_id=2, local_date=date(2026, 1, 28)),
        ]
        orch._current_all_txns = txns
        orch._current_day_txns = txns

        result = orch.phase_retention(date(2026, 1, 31))

        assert result["total_clients"] == 2
        assert result["active"] >= 1
        assert "alerts" in result

    def test_retention_phase_in_pipeline(self):
        """RETENTION should appear in the pipeline phases."""
        from app.services.orchestrator import Orchestrator, Phase

        config = self._make_config()

        # Mock parser that returns some transactions
        txns = [
            self._make_txn("A1234A", station_id=3, local_date=date(2026, 1, 29)),
            self._make_txn("T5678", station_id=2, local_date=date(2026, 1, 29)),
        ]

        class MockParseResult:
            def __init__(self, txns):
                self.transactions = txns
                self.row_count = len(txns)

        def mock_parser(csv_dir):
            return [MockParseResult(txns)]

        orch = Orchestrator(config, parser=mock_parser)
        result = orch.run_daily_close(date(2026, 1, 29))

        assert Phase.RETENTION.value in result.phases
        retention_phase = result.phases[Phase.RETENTION.value]
        # Should succeed (retention analysis doesn't need DB/Odoo)
        assert retention_phase.status.value == "success"

    def test_retention_whatsapp_msg_stored(self):
        """After retention phase, WhatsApp message should be available."""
        from app.services.orchestrator import Orchestrator

        config = self._make_config()
        orch = Orchestrator(config)

        txns = [
            self._make_txn("TEST-001", station_id=3, local_date=date(2026, 1, 25)),
        ]
        orch._current_all_txns = txns

        orch.phase_retention(date(2026, 1, 31))

        assert hasattr(orch, "_retention_whatsapp_msg")
        assert len(orch._retention_whatsapp_msg) > 0
        assert "REPORTE RETENCIÓN" in orch._retention_whatsapp_msg

    def test_market_profiles_used(self):
        """Market profiles should influence segment classification."""
        from app.services.orchestrator import Orchestrator

        market_profiles = {
            "TAXI-X": {"segmento": "TAXI"},
        }

        config = self._make_config()
        orch = Orchestrator(config, market_profiles=market_profiles)

        txns = [
            self._make_txn("TAXI-X", station_id=2, local_date=date(2026, 1, 29)),
        ]
        orch._current_all_txns = txns

        orch.phase_retention(date(2026, 1, 31))

        profile = orch._retention_profiles["TAXI-X"]
        from app.models.client import Segmento
        assert profile.segmento == Segmento.TAXI


# ============================================================
# Integration with Real NatGas File (skip in CI)
# ============================================================

NATGAS_PATH = Path(
    "/sessions/cool-focused-pasteur/mnt/Downloads/"
    "CMU/Estados_Cuenta/Combis AGS JH.xlsx"
)


@pytest.mark.skipif(
    not NATGAS_PATH.exists(),
    reason="NatGas master database not available"
)
class TestNatGasIntegration:
    """Integration tests with real NatGas Excel data."""

    def test_load_real_natgas(self):
        from app.services.market_loader import load_natgas_vehiculos
        from collections import Counter
        vehicles = load_natgas_vehiculos(NATGAS_PATH)
        assert len(vehicles) > 80_000  # 86,674 expected
        ags = [v for v in vehicles if v.is_ags]
        assert len(ags) > 4_000  # ~4,871 AGS vehicles expected

        # Verify segment distribution matches Josue's domain knowledge
        dist = Counter(v.market_segment.value for v in ags)
        assert dist["TAXI"] > 1500, f"Expected >1500 taxis, got {dist['TAXI']}"
        assert dist["COMBI"] < 500, f"Expected <500 combis, got {dist['COMBI']}"
        assert dist["COMBI"] > 100, f"Expected >100 combis, got {dist['COMBI']}"
        # PLATAFORMA should exist as separate from TAXI
        assert dist.get("PLATAFORMA", 0) > 50, (
            f"Expected >50 plataforma, got {dist.get('PLATAFORMA', 0)}"
        )
        # BUS = only Camión Colectivo with consumo_max > 200 (~55 real buses)
        assert dist.get("BUS", 0) <= 80, (
            f"Expected ≤80 buses (real buses ~55), got {dist.get('BUS', 0)}"
        )
        assert dist.get("BUS", 0) > 30, (
            f"Expected >30 buses, got {dist.get('BUS', 0)}"
        )
        # Log full distribution for debugging
        for seg, count in dist.most_common():
            print(f"  {seg}: {count}")

    def test_real_profiles(self):
        from app.services.market_loader import (
            load_natgas_vehiculos,
            load_natgas_consumo,
            build_existing_profiles,
        )
        vehicles = load_natgas_vehiculos(NATGAS_PATH)
        consumo = load_natgas_consumo(NATGAS_PATH)
        profiles = build_existing_profiles(vehicles, consumo)
        assert len(profiles) > 4_000
