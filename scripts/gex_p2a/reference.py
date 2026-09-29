"""Independent Decimal reference and conditional float64 forward-error bounds.

No GRID/Gamma Watch imports. Inputs are exact binary64 values, not idealized
decimal literals. Ball arithmetic propagates intervals and adds a rounding
allowance at EVERY operation; transcendental functions assume <= 2 ulp error.
This is a conditional engineering bound, not a libm correctness certificate.
"""

from decimal import Decimal as D, localcontext
import math

PI = D(
    "3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825342117067982148086513282306647"
)
U = D.from_float(2.0**-53)
SUB = D.from_float(math.ulp(0.0))


def dec(x):
    return x if isinstance(x, D) else D.from_float(float(x))


def gamma(s, k, t, r, q, iv, precision=80):
    """High precision, no T floor, no IV fallback or position assumption."""
    with localcontext() as ctx:
        ctx.prec = precision
        s, k, t, r, q, iv = map(dec, (s, k, t, r, q, iv))
        d = ((s / k).ln() + (r - q + iv * iv / 2) * t) / (iv * t.sqrt())
        return (-q * t - d * d / 2).exp() / (s * iv * (2 * PI * t).sqrt())


class Ball:
    """Interval around a high-precision center with float rounding propagated."""

    def __init__(self, value, radius=0):
        self.v, self.e = dec(value), dec(radius)

    @property
    def lo(self):
        return self.v - self.e

    @property
    def hi(self):
        return self.v + self.e

    @staticmethod
    def cast(x):
        return x if isinstance(x, Ball) else Ball(x)

    @classmethod
    def enclose(cls, center, lo, hi, lib=False):
        # RN basic op <= u/(1-u)*|result|; 2 ulp <= 4u/(1-4u)*|result|.
        # One smallest subnormal also covers absolute underflow rounding.
        factor = 4 * U if lib else U
        scale = max(abs(lo), abs(hi))
        rounding = factor / (1 - factor) * scale + SUB
        # Decimal evaluation guard at the active working precision.
        from decimal import getcontext

        guard = D(10) ** (-getcontext().prec + 3) * max(D(1), scale)
        return cls(center, max(abs(center - lo), abs(hi - center)) + rounding + guard)

    def __add__(self, other):
        b = self.cast(other)
        return self.enclose(self.v + b.v, self.lo + b.lo, self.hi + b.hi)

    __radd__ = __add__

    def __neg__(self):
        return Ball(-self.v, self.e)  # sign-bit change is exact

    def __sub__(self, other):
        return self + -self.cast(other)

    def __rsub__(self, other):
        return self.cast(other) - self

    def __mul__(self, other):
        b = self.cast(other)
        ends = [x * y for x in (self.lo, self.hi) for y in (b.lo, b.hi)]
        return self.enclose(self.v * b.v, min(ends), max(ends))

    __rmul__ = __mul__

    def __truediv__(self, other):
        b = self.cast(other)
        if b.lo <= 0 <= b.hi:
            raise ValueError("error interval spans zero denominator")
        ends = [x / y for x in (self.lo, self.hi) for y in (b.lo, b.hi)]
        return self.enclose(self.v / b.v, min(ends), max(ends))

    def __rtruediv__(self, other):
        return self.cast(other) / self

    def __pow__(self, power):
        if power != 2:
            raise ValueError("only square is required by this protocol")
        return self * self

    def unary(self, name):
        method = {"exp": "exp", "log": "ln", "sqrt": "sqrt"}[name]
        return self.enclose(
            getattr(self.v, method)(),
            getattr(self.lo, method)(),
            getattr(self.hi, method)(),
            lib=True,
        )


class BoundMath:
    # Engines use binary64 pi; include its difference from reference pi.
    pi = Ball(PI, abs(PI - D.from_float(math.pi)))

    @staticmethod
    def exp(x):
        return Ball.cast(x).unary("exp")

    @staticmethod
    def log(x):
        return Ball.cast(x).unary("log")

    @staticmethod
    def sqrt(x):
        return Ball.cast(x).unary("sqrt")


def reference_bound(s, k, t, r, q, iv):
    """Operation tree for GRID primitive gamma; no empirical tolerance fitting."""
    s, k, t, r, q, iv = map(Ball, (s, k, t, r, q, iv))
    root = BoundMath.sqrt(t)
    d = (BoundMath.log(s / k) + (r - q + 0.5 * iv**2) * t) / (iv * root)
    pdf = BoundMath.exp(-0.5 * d * d) / BoundMath.sqrt(2 * BoundMath.pi)
    return BoundMath.exp(-q * t) * pdf / (s * iv * root)
