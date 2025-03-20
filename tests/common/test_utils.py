from src.common.utils import get_is_history


def test_get_is_history():
    args = ["run.sh", "dev", "2024-10-10", "true"]
    expected = True
    actual = get_is_history(args)
    actual == expected

    args = ["run.sh", "dev", "2024-10-10", "false"]
    expected = False
    actual = get_is_history(args)
    actual == expected

    args = ["run.sh", "dev", "2024-10-10"]
    expected = False
    actual = get_is_history(args)
    actual == expected
