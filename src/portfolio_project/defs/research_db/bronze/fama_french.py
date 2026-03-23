import sys

from portfolio_project.defs.portfolio_db.bronze import fama_french as _legacy_module

sys.modules[__name__] = _legacy_module
