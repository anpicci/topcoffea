import unittest

from topcoffea.modules.utils import (
    cached_get_correlation_tag,
    cached_get_syst,
    cached_get_syst_lst,
    canonicalize_process_name,
)


class CanonicalizeProcessNameTests(unittest.TestCase):
    def test_lowercases_leading_token(self):
        self.assertEqual(canonicalize_process_name("NonPromptUL16"), "nonpromptUL16")

    def test_preserves_trailing_caps_suffix(self):
        self.assertEqual(canonicalize_process_name("Flips2023BPix"), "flips2023BPix")

    def test_returns_unmodified_when_no_alpha_prefix(self):
        self.assertEqual(canonicalize_process_name("123abc"), "123abc")


class RateSystematicHelpersTests(unittest.TestCase):
    def test_cached_get_syst_defaults_and_range_parsing(self):
        self.assertEqual(cached_get_syst("nonexistent"), (1.0, 1.0, 0))

        lumi_down, lumi_up, _ = cached_get_syst("lumi")
        self.assertAlmostEqual(lumi_down, 0.984, places=6)
        self.assertAlmostEqual(lumi_up, 1.016, places=6)

        flips_down, flips_up, _ = cached_get_syst("charge_flips", "charge_flips_sm")
        self.assertAlmostEqual(flips_down, 0.7, places=6)
        self.assertAlmostEqual(flips_up, 1.3, places=6)

        self.assertEqual(cached_get_syst("pdf_scale", "tttt"), (0.9312, 1.0688, 0))

    def test_cached_get_syst_lst(self):
        systs = cached_get_syst_lst()
        self.assertIn("lumi", systs)
        self.assertIn("qcd_scale", systs)

    def test_cached_get_correlation_tag(self):
        self.assertEqual(cached_get_correlation_tag("pdf_scale", "ttH"), "gg")
        self.assertIsNone(cached_get_correlation_tag("charge_flips", "ttH"))

if __name__ == "__main__":
    unittest.main()
