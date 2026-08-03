from pathlib import Path


REPO_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "code" / "src").is_dir()
)
START_ENDPOINTS = REPO_ROOT / "deploy" / "autodl" / "start_endpoints.sh"
AUTODL_ENV_EXAMPLE = REPO_ROOT / "deploy" / "autodl" / "autodl.env.example"


def test_start_endpoints_exposes_venv_tools_to_vllm_subprocesses() -> None:
    script = START_ENDPOINTS.read_text(encoding="utf-8")

    path_export = 'export PATH="$VLLM_VENV/bin:$PATH"'
    python_assignment = 'PYTHON="$VLLM_VENV/bin/python"'

    assert path_export in script
    assert script.index(path_export) < script.index(python_assignment)


def test_autodl_env_uses_one_cuda_toolkit_for_flashinfer_jit() -> None:
    env_example = AUTODL_ENV_EXAMPLE.read_text(encoding="utf-8")

    assert "CUDA_HOME=/usr/local/cuda-13.0" in env_example
    assert "CUDA_NVCC_BIN=/usr/local/cuda-13.0/bin" in env_example
