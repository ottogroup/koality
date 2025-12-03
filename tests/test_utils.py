import datetime as dt

import pytest


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("test string", "test string"),
        ("Cr4zy-str!ng11", "Cr4zy-str!ng11"),
        ("TRUE", True),
        ("false", False),
        ("3.1415", 3.1415),
        ("1993", 1993),
        ("0", 0),
        ("0.0", 0.0),
    ],
)
def test_parse_arg(test_input, expected):
    from koality.utils import parse_arg

    assert expected == parse_arg(test_input)


class TestResolveDottedName:
    @pytest.fixture()
    def resolve_dotted_name(self):
        from koality.utils import resolve_dotted_name

        return resolve_dotted_name

    def test_resolve_dotted_name_colon(self, resolve_dotted_name):
        import koality

        assert (
            resolve_dotted_name("koality.tests.test_utils:TestResolveDottedName.test_resolve_dotted_name_colon")
            is koality.tests.test_utils.TestResolveDottedName.test_resolve_dotted_name_colon
        )

    def test_resolve_dotted_name(self, resolve_dotted_name):
        import koality

        assert (
            resolve_dotted_name("koality.tests.test_utils.TestResolveDottedName")
            is koality.tests.test_utils.TestResolveDottedName
        )

    def test_resolve_dotted_name_koality_check(self, resolve_dotted_name):
        from koality.checks import NullRatioCheck

        assert resolve_dotted_name("NullRatioCheck") is NullRatioCheck


@pytest.mark.parametrize(
    "input_date, offset, expected",
    [
        ("today", 0, dt.date.today().isoformat()),
        ("yesterday", 0, (dt.date.today() - dt.timedelta(days=1)).isoformat()),
        ("tomorrow", 0, (dt.date.today() + dt.timedelta(days=1)).isoformat()),
        ("today", 1, (dt.date.today() + dt.timedelta(days=1)).isoformat()),
        ("yesterday", 1, dt.date.today().isoformat()),
        ("tomorrow", 1, (dt.date.today() + dt.timedelta(days=2)).isoformat()),
        ("today", -2, (dt.date.today() - dt.timedelta(days=2)).isoformat()),
        ("yesterday", -2, (dt.date.today() - dt.timedelta(days=3)).isoformat()),
        ("tomorrow", -2, (dt.date.today() - dt.timedelta(days=1)).isoformat()),
        ("19901003", 0, "1990-10-03"),
        ("19901003", 5, "1990-10-08"),
        ("1990-10-03", 0, "1990-10-03"),
    ],
)
def test_parse_date(input_date, offset, expected):
    from koality.utils import parse_date

    assert expected == parse_date(input_date, offset)


class TestParseGCSPath:
    @pytest.fixture()
    def parse_gcs_path(self):
        from koality.utils import parse_gcs_path

        return parse_gcs_path

    @pytest.mark.parametrize(
        "test_input, expected",
        [
            (
                "gs://bucket-name/important-data.csv",
                (
                    "bucket-name",
                    "important-data.csv",
                ),
            ),
            (
                "gs://bucket-name/folder_name/important-data.csv",
                (
                    "bucket-name",
                    "folder_name/important-data.csv",
                ),
            ),
        ],
    )
    def test_parse_gcs_path(self, parse_gcs_path, test_input, expected):
        assert parse_gcs_path(test_input) == expected

    def test_parse_gcs_path_error(self, parse_gcs_path):
        with pytest.raises(ValueError) as exc:
            parse_gcs_path("I am a long text and should raise an exception!")

        assert str(exc.value) == "A GCS path needs to start with gs://"

        with pytest.raises(ValueError) as exc:
            parse_gcs_path("gs://bucket-name")

        assert str(exc.value) == "Blob name is empty"


class TestEnsureSetString:
    @pytest.fixture
    def to_set(self):
        from koality.utils import to_set

        return to_set

    @pytest.mark.parametrize(
        "test_input, expected",
        [
            ('("toys", "clothing")', {"clothing", "toys"}),
            ('("toys")', {"toys"}),
            ('"toys"', {"toys"}),
            ("toys", {"toys"}),
            ('("toys", "toys", "clothing")', {"clothing", "toys"}),
            ('("clothing", "toys")', {"clothing", "toys"}),
            (True, {True}),
            (1, {1}),
            (["toys"], {"toys"}),
            ({"toys"}, {"toys"}),
        ],
    )
    def test_to_set(self, to_set, test_input, expected):
        assert to_set(test_input) == expected


class TestFormatDynamic:
    @pytest.fixture
    def format_dynamic(self):
        from koality.utils import format_dynamic

        return format_dynamic

    @pytest.mark.parametrize(
        "value, min_precision, output",
        [
            (0, 4, "0"),
            (0.0, 4, "0"),
            (0.1, 4, "0.1"),
            (0.0001, 4, "0.0001"),
            (0.00009, 4, "0.0001"),
            (0.000001, 4, "0.000001"),
            (0.000101, 4, "0.0001"),
            (10.0001, 4, "10.0001"),
            (10.000001, 4, "10"),
            #TODO Look at again maybe use diff then None?
            (None, 4, "nan"),
            (None, 4, "inf"),
            (None, 4, "-inf"),
            (None, 4, "nan"),
            (None, 4, "nan"),
            # non default precicions
            (0.103456789, 1, "0.1"),
            (0.103456789, 2, "0.1"),
            (0.103456789, 3, "0.103"),
            (0.103456789, 4, "0.1035"),
            (-0.103456789, 4, "-0.1035"),
            (0.10345678900, 9, "0.103456789"),
            (0.10345678900, 10, "0.103456789"),
        ],
    )
    def test_format_dynamic(self, format_dynamic, value, output, min_precision):
        assert format_dynamic(value, min_precision) == output

    def test_format_dynamic_default(self, format_dynamic):
        assert format_dynamic(0.000001) == "0.000001"
        assert format_dynamic(0.000101) == "0.0001"
        assert format_dynamic(10.0001) == "10.0001"
        assert format_dynamic(10.000001) == "10"
        #TODO Look at again
        assert format_dynamic(None) == "nan"
        assert format_dynamic(None) == "inf"
        assert format_dynamic(None) == "-inf"
        assert format_dynamic(None) == "nan"
        #End of TODO
        assert format_dynamic(None) == "nan"

    def test_format_dynamic_false_min_precision(self, format_dynamic):
        with pytest.raises(ValueError) as exc:
            format_dynamic(0.1234, -5)

        assert str(exc.value) == "min_precision must be >= 1"
