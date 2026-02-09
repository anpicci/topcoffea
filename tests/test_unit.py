import numpy as np
import awkward as ak
from topcoffea.modules.histEFT import HistEFT
from topcoffea.modules.WCPoint import WCPoint
from topcoffea.modules.WCFit import WCFit


def fval(xvals = [], svals = []):
    # Ordering convention for the structure constants:
    # Dim=0 (0,0)
    # Dim=1 (0,0) (1,0) (1,1)
    # Dim=2 (0,0) (1,0) (1,1) (2,0) (2,1) (2,2)
    y = 0.0
    idx = 0
    for i in range(len(xvals)):
        for j in range(i+1):
            c1 = xvals[i]
            c2 = xvals[j]
            s  = svals[idx]
            y += s*c1*c2
            #print(f'{i},{j} ')
            idx += 1
    #print()
    return y

########################### WCFit unit tests ###########################

def test_wcfit():
    chk_str = ''

    unit_chk = True
    all_chks,units = [0]*2
    tolerance = 1e-4

    # The structure constants
    s00 = 1.0
    s10 = 1.5
    s11 = 1.25

    pts = []
    vals = [-1.0,1.25,0.5,2.5,4]
    for x in vals:
        y = s00*1.0 + s10*x + s11*x*x
        pts.append(WCPoint(f'EFTrwgt0_ctG_{x}',y))

    chk_x = 1.5
    chk_y = s00*1.0 + s10*chk_x + s11*chk_x*chk_x
    chk_pt = WCPoint(f'EFTrwgt0_ctG_{chk_x}',0.0)

    print('Running unit tests for WCFit class')
    all_chks = 0
    units = 0

    fit_base = WCFit(pts,'base')
    unit_chk = (abs(fit_base.EvalPoint(chk_pt) - chk_y) < tolerance)
    all_chks += unit_chk
    units += 1

    chk_str = 'Passed' if unit_chk else 'Failed'
    print('--- UNIT 1 ---')
    print('chk_x    : ', chk_x)
    print('chk_y    : ', chk_y)
    print('EvalPoint: ', fit_base.EvalPoint(chk_pt))
    print('test: ', chk_str)
    print('--------------\n')

    fit_new = WCFit()
    fit_new.SetTag('new')
    fit_new.AddFit(fit_base)
    unit_chk = (abs(fit_base.EvalPoint(chk_pt) - chk_y) < tolerance)
    all_chks += unit_chk
    units += 1

    chk_str = 'Passed' if unit_chk else 'Failed'
    print('--- UNIT 2 ---')
    print('chk_x    : ', chk_x)
    print('chk_y    : ', chk_y)
    print('EvalPoint: ', fit_new.EvalPoint(chk_pt))
    print('test: ', chk_str)
    print('--------------\n')

    fit_new.AddFit(fit_base) #CAREFUL b/c WCFit is mutable
    unit_chk = (abs(fit_new.EvalPoint(chk_pt) - 2*chk_y) < tolerance)
    all_chks += unit_chk
    units += 1

    chk_str = 'Passed' if unit_chk else 'Failed'
    print('--- UNIT 3 ---')
    print('chk_x    : ', chk_x)
    print('chk_y    : ', 2*chk_y)
    print('EvalPoint: ', fit_new.EvalPoint(chk_pt))
    print('test: ', chk_str)
    print('--------------\n')

    #fit_base = WCFit(pts,'base') #redefine b/c WCFit is mutable
    unit_chk = (abs(fit_base.EvalPoint(chk_pt) - chk_y) < tolerance)
    all_chks += unit_chk
    units += 1

    chk_str = 'Passed' if unit_chk else 'Failed'
    print('--- UNIT 4 ---')
    print('chk_x    : ', chk_x)
    print('chk_y    : ', chk_y)
    print('EvalPoint: ', fit_base.EvalPoint(chk_pt))
    print('test: ', chk_str)
    print('--------------\n')

    print(f'Passed Checks: {all_chks}/{units}')
    assert (all_chks == units)

########################### Stats unit tests ###########################

def test_stats():
    chk_str = ''
    unit_chk = True
    all_chks,units = [0]*2
    result,expected,diff,tolerance = [0]*4
    tolerance = 0.001

    # Basically the SM 'strength'
    x0 = 1.0

    # Dummy WC names to use (needs to match dimension of pt
    wc_names = ['sm','ctG','ctZ']

    # The structure constants, need to match dimension of pt
    svals = [
        1.15, # (00)
        1.35,1.25, # (10) (11)
        0.25,0.75,1.00, # (20) (21) (22)
    ]
    # Make sure there are enough pts to fully determine the fit!
    pts = [
        [x0,-1.00, 0.00],
        [x0,-0.50, 0.25],
        [x0, 0.00, 0.35],
        [x0, 0.25, 0.05],
        [x0, 0.50,-0.05],
        [x0, 0.75, 0.25],
        [x0, 1.00,-0.35],
    ]

    wc_pts = []
    idx=0
    for pt in pts:
        y = fval(pt,svals)
        s = f'EFTrwgt{idx}'
        for i in range(1, len(pt)): # NOTE: pt better not be size 0!!
            wc_str = wc_names[i]
            s += f'_{wc_str}_{pt[i]}'
        #print(s,y)
        wc_pts.append(WCPoint(s,y))
        idx += 1

    fit_1 = WCFit(wc_pts,'f1')
    fit_2 = WCFit()
    fit_2.SetTag('f2')

    nevents = 5000
    for i in range(nevents):
        fit_2.AddFit(fit_1)

    ###########################

    print('Running unit tests for stats unc.')
    all_chks = 0
    units = 0

    # Needs to be the same size as wc_names
    chk_x = [x0,1.2,0.4]
    chk_y = 0.0
    chk_e = 0.0
    for i in range(nevents):
        v = fval(chk_x,svals)
        chk_y += v
        chk_e += v*v
    chk_e = chk_e**.5

    chk_wcstr = 'EFTrwgt0'
    sidx = 0
    for i in range(len(wc_names)):
        if i: # Need to skip first entry since that's the SM 'strength'
            chk_wcstr += f'_{wc_names[i]}_{chk_x[i]}'
        for j in range(i+1):
            v = svals[sidx]
            #print(f'{i}{j}: {v}')
            sidx = sidx + 1
    print()
    chk_pt = WCPoint(chk_wcstr,0.0)

    ###########################

    # Basic check for proper adding of quadratic structure constants
    # Note: We expect the diff to grow with increased number of events due to the numeric precison
    expected = chk_y
    result = fit_2.EvalPoint(chk_pt)
    diff = abs(expected - result)
    tolerance = 1e-4

    unit_chk = (diff < tolerance)
    all_chks += unit_chk
    units += 1
    chk_str = 'Passed' if unit_chk else 'Failed'
    print('--- UNIT 1 ---')
    print('evts     : ', nevents)
    print('chk_wcstr: ', chk_wcstr)
    print('expected : ', expected)
    print('result   : ', result)
    print('diff     : ', diff)
    print('tolerance: ', tolerance)
    print('test: ', chk_str)
    print('--------------\n')


    # Check the error calculation
    # Note: We expect the diff to grow with increased number of events due to the numeric precison
    expected = chk_e
    result = fit_2.EvalPointError(chk_pt)
    diff = abs(expected - result)
    tolerance = 1e-05*(10*nevents)**.5

    unit_chk = (diff < tolerance)
    all_chks += unit_chk
    units += 1
    chk_str = 'Passed' if unit_chk else 'Failed'
    print('--- UNIT 2 ---')
    print('evts     : ', nevents)
    print('chk_wcstr: ', chk_wcstr)
    print('expected : ', expected)
    print('result   : ', result)
    print('diff     : ', diff)
    print('tolerance: ', tolerance)
    print('test: ', chk_str)
    print('--------------\n')

    # Now do the percent error
    # Note: The diff here also appears to grow apparently due to numeric precison, but much more slowly (it is still kind of concerning)
    expected = chk_e / chk_y
    result = fit_2.EvalPointError(chk_pt) / fit_2.EvalPoint(chk_pt)
    diff = abs(expected - result)
    tolerance = 1e-04

    unit_chk = (diff < tolerance)
    all_chks += unit_chk
    units += 1
    chk_str = 'Passed' if unit_chk else 'Failed'
    print('--- UNIT 3 ---')
    print('evts     : ', nevents)
    print('chk_wcstr: ', chk_wcstr)
    print('expected : ', expected)
    print('result   : ', result)
    print('diff     : ', diff)
    print('tolerance: ', tolerance)
    print('test: ', chk_str)
    print('--------------\n')

    ###########################

    print(f'Passed Checks: {all_chks}/{units}')
    assert (all_chks == units)

########################### HistEFT unit tests ###########################

def test_histeft():
    import hist as modern_hist

    h_base = HistEFT(
        modern_hist.axis.StrCategory([], name="process", growth=True),
        modern_hist.axis.Regular(2, 0, 2, name="observable"),
        wc_names=["ctG", "ctZ"],
        label="Events",
    )

    eft_coeff = np.array(
        [
            [1.00, 0.40, 0.20, -0.10, 0.05, 0.30],
            [0.70, -0.20, 0.60, 0.10, -0.15, 0.25],
        ],
        dtype=float,
    )
    h_base.fill(
        process="signal",
        observable=np.array([0.5, 1.5], dtype=float),
        eft_coeff=eft_coeff,
    )
    h_base.fill(
        process="background",
        observable=np.array([0.5], dtype=float),
        weight=np.array([2.0], dtype=float),
    )

    eval_map = h_base.eval({"ctG": 0.25, "ctZ": -0.10})
    eval_arr = h_base.eval(np.array([0.25, -0.10], dtype=float))
    assert np.allclose(eval_map[("signal",)], eval_arr[("signal",)])

    as_hist = h_base.as_hist({"ctG": 0.25, "ctZ": -0.10})
    assert isinstance(as_hist, modern_hist.Hist)
    assert list(as_hist.axes.name) == ["process", "observable"]

    grouped = h_base.group("process", {"all": ["signal", "background"]})
    total_before = h_base.integrate("process").eval({"ctG": 0.2, "ctZ": -0.3})[()].sum()
    total_after = grouped.integrate("process").eval({"ctG": 0.2, "ctZ": -0.3})[()].sum()
    assert np.isclose(total_before, total_after)

    signal = h_base.integrate("process", "signal")
    coeffs = signal.view(as_dict=True)[()]
    wc_val = 0.6
    sm_idx = signal.quadratic_term_index("sm", "sm")
    lin_idx = signal.quadratic_term_index("sm", "ctG")
    quad_idx = signal.quadratic_term_index("ctG", "ctG")
    expected = (
        coeffs[:, sm_idx].sum()
        + coeffs[:, lin_idx].sum() * wc_val
        + coeffs[:, quad_idx].sum() * wc_val * wc_val
    )
    observed = signal.eval({"ctG": wc_val, "ctZ": 0.0})[()].sum()
    assert np.isclose(expected, observed)
