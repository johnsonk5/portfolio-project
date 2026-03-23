import sys

from portfolio_project.defs.portfolio_db.silver import research_prices as _legacy_module

sys.modules[__name__] = _legacy_module
