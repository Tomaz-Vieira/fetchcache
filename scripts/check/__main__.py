from scripts import SOURCE_DIR, SUPPORTED_PYTHON_VERSIONS, TESTS_DIR, pyright_check

for py_ver in SUPPORTED_PYTHON_VERSIONS:
    result = pyright_check(
        py_ver=py_ver,
        no_dev=True,
        directory=SOURCE_DIR
    )
    if result.returncode != 0:
        exit(result.returncode)

    result = pyright_check(
        py_ver=py_ver,
        no_dev=False,
        directory=TESTS_DIR,
    )
    if result.returncode != 0:
        exit(result.returncode)

