from hail.expr.expressions import *
from hail.expr.expressions.expression_typecheck import *
from hail.expr.types import *
from hail.typecheck import *
from hail.expr.functions import _func


# @typecheck(gt_counts=expr_array(expr_int32))
# def haplotype_freq_em(n00, n01, n02, n10, n11, n12, n20, n21, n22) -> Float64Expression:
def haplotype_freq_em(gt_counts) -> Float64Expression:
    return _func("prob_same_hap_em", tfloat64, gt_counts)