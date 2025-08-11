from scripts import PROJECT_ROOT, SUPPORTED_PYTHON_VERSIONS, TESTS_DIR, uv_run

for py_ver in SUPPORTED_PYTHON_VERSIONS:
    for entry in TESTS_DIR.iterdir():
        if not entry.is_dir():
            continue
        if not entry.name.startswith("test_"):
            continue
        module_dotted_path = entry.relative_to(PROJECT_ROOT).as_posix().replace("/", ".")
        result = uv_run(py_ver=py_ver, no_dev=False, command=["python3", "-m", module_dotted_path])
        if result.returncode != 0:
            exit(result.returncode)

