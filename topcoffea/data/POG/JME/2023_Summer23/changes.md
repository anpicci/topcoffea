## Changes: 2026-07-15 (\[Summer23Prompt23_RunCv1234_JRV2\] Minor fix to the JER SF Uncertainty)

Merge Request: [!4](https://gitlab.cern.ch/cms-analysis-corrections/JME/Run3-23CSep23-Summer23-NanoAODv12/-/merge_requests/4)

In the current JSON format, JEC and JER uncertainties are stored as symmetric values. With `JetEta` and `JetPt` as inputs, the JSON returns a single `unc` value, which is then applied symmetrically (e.g., `SF(up/down) = SF(nom) x (1 +/- unc)`).

For the 2016-2022 campaigns, the JER SF uncertainties in the TXT files were symmetric, so choosing either the "up" or "down" column during JSON conversion did not affect the result. However, the 2023-2026 JER `SFUncertainty` TXT files contain asymmetric uncertainties. In edge cases (such as high $\eta$ and low $p_T$), a SF might be evaluated as `1.012^{+0.165}_{-0.012}`. This asymmetry occurs because the down uncertainty was artificially bounded to prevent `SF(down)` from falling below 1.0. For the bulk of the typical analysis phase space, the "up" and "down" uncertainties are approximately symmetric, making this a relatively minor effect for most analyses.

To preserve the true magnitude of the systematic error, the safest approach is to store the unbounded "up" uncertainty in the JSON. While this is already implemented for the 2024-2026 JSONs (in the [2024](https://gitlab.cern.ch/cms-analysis-corrections/JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/-/merge_requests/3), [2025](https://gitlab.cern.ch/cms-analysis-corrections/JME/Run3-25Prompt-Summer24-NanoAODv15/-/merge_requests/3), and [2026](https://gitlab.cern.ch/cms-analysis-corrections/JME/Run3-26Prompt-Summer24-NanoAODv15/-/merge_requests/1) MRs), it was missed for the 2023 files.

This MR fixes this minor issue in the conversion script, ensuring we now consistently retrieve and store the JER SF "up" uncertainty in the JSON files. The new tag version is: `Summer23Prompt23_RunCv1234_JRV3_MC`.
