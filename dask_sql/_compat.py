import prompt_toolkit
from packaging.version import parse as parseVersion

_prompt_toolkit_version = parseVersion(prompt_toolkit.__version__)

# TODO: remove if prompt-toolkit min version gets bumped
PIPE_INPUT_CONTEXT_MANAGER = _prompt_toolkit_version >= parseVersion("3.0.29")
