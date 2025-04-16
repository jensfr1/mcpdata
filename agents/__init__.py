# This file makes the agents directory a Python package
# We don't directly import the functions since they're defined inside register functions
# Instead, we'll import the register functions
from .steward import register as register_steward
from .emma import register as register_emma
from .oskar import register as register_oskar
from .mathias import register as register_mathias
from .james import register as register_james
from .gina import register as register_gina
from .mike import register as register_mike 