from hail.expr.expressions import *
from hail.expr.expressions.expression_typecheck import *
from hail.expr.types import *
from hail.typecheck import *
from hail.expr.functions import _func


@typecheck(gt_counts=expr_array(expr_int32))
def haplotype_freq_em(gt_counts) -> ArrayExpression:
    return _func("haplotype_freq_em", tarray(tfloat64), gt_counts)