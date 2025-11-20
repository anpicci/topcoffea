import unittest

from topcoffea.modules.utils import canonicalize_process_name


class CanonicalizeProcessNameTests(unittest.TestCase):
    def test_lowercases_leading_token(self):
        self.assertEqual(canonicalize_process_name("NonPromptUL16"), "nonpromptUL16")

    def test_preserves_trailing_caps_suffix(self):
        self.assertEqual(canonicalize_process_name("Flips2023BPix"), "flips2023BPix")

    def test_returns_unmodified_when_no_alpha_prefix(self):
        self.assertEqual(canonicalize_process_name("123abc"), "123abc")
if __name__ == "__main__":
    unittest.main()
