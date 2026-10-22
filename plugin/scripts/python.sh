# Sourced by the lifecycle hooks (not run on its own): find a Python 3 that
# actually runs and name it in PY, or leave PY empty.
#
# `command -v python3` is not enough. On Windows, python3 is often the
# Microsoft Store alias, which is found on the PATH and exits 49 without
# running anything, so a hook that trusted the lookup would fail later
# instead of taking its no-Python path. Each candidate is run once.
if [ -z "${PY:-}" ]; then
    for _acs_py in python3 python; do
        if "$_acs_py" -c 'import sys; sys.exit(0 if sys.version_info[0] >= 3 else 1)' \
            >/dev/null 2>&1; then
            PY="$_acs_py"
            break
        fi
    done
    unset _acs_py
fi
PY="${PY:-}"
