"""Tests for core check implementations."""

import pytest
import pandas as pd
import numpy as np
from data_quality.checks.completeness import CompletenessCheck
from data_quality.checks.uniqueness import UniquenessCheck
from data_quality.checks.range_check import RangeCheck
from data_quality.checks.turnover import TurnoverCheck
from data_quality.utils.constants import CheckStatus


class TestCompletenessCheck:
    """Tests for CompletenessCheck."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with nulls."""
        return pd.DataFrame({
            'entity_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'effective_date': pd.to_datetime(['2025-01-01'] * 10),
            'value': [100.0, 200.0, np.nan, 150.0, np.nan, 175.0, 180.0, np.nan, 190.0, 200.0],
            'category': ['A', 'B', None, 'A', 'B', None, 'A', 'B', 'A', None],
            'universe': ['US', 'US', 'EU', 'US', 'EU', 'US', 'EU', 'US', 'EU', 'US'],
        })

    def test_completeness_pass(self, sample_df):
        """Test completeness check passes when under threshold."""
        config = {
            'value': {
                'thresholds': {'absolute_critical': 0.50},  # 50% threshold
                'description': 'Value completeness'
            }
        }

        check = CompletenessCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        assert results.iloc[0]['status'] == CheckStatus.PASS

    def test_completeness_fail(self, sample_df):
        """Test completeness check fails when over threshold."""
        config = {
            'value': {
                'thresholds': {'absolute_critical': 0.10},  # 10% threshold
                'description': 'Value completeness'
            }
        }

        check = CompletenessCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        # 3 nulls out of 10 = 30%, should fail
        assert results.iloc[0]['status'] == CheckStatus.FAIL

    def test_completeness_with_filter(self, sample_df):
        """Test completeness with filter condition."""
        config = {
            'value': {
                'thresholds': {'absolute_critical': 0.50},
                'filter_condition': "universe == 'US'",
                'description': 'Value completeness for US'
            }
        }

        check = CompletenessCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        # Filter applied

    def test_completeness_multiple_columns(self, sample_df):
        """Test completeness for multiple columns."""
        config = {
            'value': {
                'thresholds': {'absolute_critical': 0.50}
            },
            'category': {
                'thresholds': {'absolute_critical': 0.50}
            }
        }

        check = CompletenessCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) == 2


class TestUniquenessCheck:
    """Tests for UniquenessCheck."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with duplicates."""
        return pd.DataFrame({
            'entity_id': [1, 2, 3, 4, 5, 5, 6, 7, 8, 8],  # 5 and 8 duplicated
            'effective_date': pd.to_datetime(['2025-01-01'] * 10),
            'value': [100, 200, 300, 400, 500, 500, 600, 700, 800, 800],
        })

    def test_uniqueness_pass(self, sample_df):
        """Test uniqueness check passes when few duplicates."""
        config = {
            'entity_id': {
                'thresholds': {'absolute_critical': 5},  # Allow up to 5 duplicates
                'description': 'Entity ID uniqueness'
            }
        }

        check = UniquenessCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        # 2 duplicate values, threshold is 5
        assert results.iloc[0]['status'] == CheckStatus.PASS

    def test_uniqueness_fail(self, sample_df):
        """Test uniqueness check fails when too many duplicates."""
        config = {
            'entity_id': {
                'thresholds': {'absolute_critical': 0},  # No duplicates allowed
                'description': 'Entity ID uniqueness'
            }
        }

        check = UniquenessCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        assert results.iloc[0]['status'] == CheckStatus.FAIL

    def test_uniqueness_no_duplicates(self):
        """Test uniqueness check with no duplicates."""
        df = pd.DataFrame({
            'entity_id': [1, 2, 3, 4, 5],
            'effective_date': pd.to_datetime(['2025-01-01'] * 5),
        })

        config = {
            'entity_id': {
                'thresholds': {'absolute_critical': 0},
                'description': 'Entity ID uniqueness'
            }
        }

        check = UniquenessCheck(
            df=df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        assert results.iloc[0]['status'] == CheckStatus.PASS


class TestRangeCheck:
    """Tests for RangeCheck."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with values."""
        return pd.DataFrame({
            'entity_id': [1, 2, 3, 4, 5],
            'effective_date': pd.to_datetime(['2025-01-01'] * 5),
            'score': [5.0, 8.0, 12.0, -2.0, 7.0],  # Out of range: 12 and -2
            'percentage': [50, 80, 110, 30, 95],  # Out of range: 110
        })

    def test_range_pass(self, sample_df):
        """Test range check passes when all values in range."""
        df = pd.DataFrame({
            'entity_id': [1, 2, 3],
            'effective_date': pd.to_datetime(['2025-01-01'] * 3),
            'score': [5.0, 7.0, 9.0],
        })

        config = {
            'score': {
                'min_value': 0,
                'max_value': 10,
                'description': 'Score range'
            }
        }

        check = RangeCheck(
            df=df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        assert results.iloc[0]['status'] == CheckStatus.PASS

    def test_range_fail(self, sample_df):
        """Test range check fails when values out of range."""
        config = {
            'score': {
                'min_value': 0,
                'max_value': 10,
                'description': 'Score range'
            }
        }

        check = RangeCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        assert results.iloc[0]['status'] == CheckStatus.FAIL

    def test_range_min_only(self, sample_df):
        """Test range check with only minimum value."""
        config = {
            'score': {
                'min_value': 0,
                'description': 'Score minimum'
            }
        }

        check = RangeCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        # -2 is below minimum, should fail
        assert results.iloc[0]['status'] == CheckStatus.FAIL

    def test_range_max_only(self, sample_df):
        """Test range check with only maximum value."""
        config = {
            'score': {
                'max_value': 10,
                'description': 'Score maximum'
            }
        }

        check = RangeCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        # 12 is above maximum, should fail
        assert results.iloc[0]['status'] == CheckStatus.FAIL


class TestTurnoverCheck:
    """Tests for TurnoverCheck."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with multiple dates."""
        return pd.DataFrame({
            'entity_id': [1, 2, 3, 4, 5,   # Date 1: 1-5
                         1, 2, 3, 6, 7],   # Date 2: 1-3, 6-7 (dropped 4,5; added 6,7)
            'effective_date': pd.to_datetime([
                '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01',
                '2025-01-02', '2025-01-02', '2025-01-02', '2025-01-02', '2025-01-02'
            ]),
            'value': [100, 200, 300, 400, 500, 110, 210, 310, 600, 700],
        })

    def test_turnover_pass(self, sample_df):
        """Test turnover check passes when under threshold."""
        config = {
            'entity_id': {
                'thresholds': {'absolute_critical': 1.0},  # 100% threshold
                'description': 'Entity turnover'
            }
        }

        check = TurnoverCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0

    def test_turnover_fail(self, sample_df):
        """Test turnover check fails when over threshold."""
        config = {
            'entity_id': {
                'thresholds': {'absolute_critical': 0.10},  # 10% threshold
                'description': 'Entity turnover'
            }
        }

        check = TurnoverCheck(
            df=sample_df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        # 4 changes (2 added, 2 dropped) out of 7 unique IDs = ~57%
        assert len(results) > 0

    def test_turnover_no_change(self):
        """Test turnover with no changes between dates."""
        df = pd.DataFrame({
            'entity_id': [1, 2, 3, 1, 2, 3],
            'effective_date': pd.to_datetime([
                '2025-01-01', '2025-01-01', '2025-01-01',
                '2025-01-02', '2025-01-02', '2025-01-02'
            ]),
            'value': [100, 200, 300, 110, 210, 310],
        })

        config = {
            'entity_id': {
                'thresholds': {'absolute_critical': 0.01},  # Very low threshold
                'description': 'Entity turnover'
            }
        }

        check = TurnoverCheck(
            df=df,
            date_col='effective_date',
            id_col='entity_id',
            check_config=config
        )

        results = check.run()
        assert len(results) > 0
        # No turnover, should pass
        assert results.iloc[0]['status'] == CheckStatus.PASS
