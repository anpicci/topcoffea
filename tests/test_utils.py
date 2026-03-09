import gzip
import pickle
import unittest
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from topcoffea.modules.utils import (
    cached_get_correlation_tag,
    cached_get_syst,
    cached_get_syst_lst,
    canonicalize_process_name,
    dump_dict_streaming,
)


class CanonicalizeProcessNameTests(unittest.TestCase):
    def test_lowercases_leading_token(self):
        self.assertEqual(canonicalize_process_name("NonPromptUL16"), "nonpromptUL16")

    def test_preserves_trailing_caps_suffix(self):
        self.assertEqual(canonicalize_process_name("Flips2023BPix"), "flips2023BPix")

    def test_returns_unmodified_when_no_alpha_prefix(self):
        self.assertEqual(canonicalize_process_name("123abc"), "123abc")

    def test_handles_run2_and_run3_samples(self):
        self.assertEqual(canonicalize_process_name("NonPromptUL18"), "nonpromptUL18")
        self.assertEqual(canonicalize_process_name("NonPrompt2022EE"), "nonprompt2022EE")


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


class StreamingPickleTests(unittest.TestCase):
    def test_dump_dict_streaming_roundtrip(self):
        payload = [("a", 1), ("nested", {"x": [1, 2, 3]}), ("tuple", (4, 5))]

        with NamedTemporaryFile(suffix=".pkl.gz") as tmp:
            with patch("builtins.print"):
                dump_dict_streaming(tmp.name, payload)

            with gzip.open(tmp.name, "rb") as stream:
                loaded = pickle.load(stream)

        self.assertEqual(loaded, dict(payload))


if __name__ == "__main__":
    unittest.main()
